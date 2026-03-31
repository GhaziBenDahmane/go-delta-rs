use std::io::Cursor;
use std::sync::Arc;

use arrow::datatypes::{DataType as ArrowType, Field, Schema, TimeUnit};
use arrow_json::ReaderBuilder;
use chrono::{TimeZone, Utc};
use deltalake::datafusion::prelude::SessionContext;
use deltalake::kernel::{DataType as DeltaType, PrimitiveType, StructField};
use deltalake::operations::vacuum::VacuumBuilder;
use deltalake::protocol::SaveMode;
use deltalake::DeltaOps;
use serde_json::Value;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::delta_proto::{
    delta_service_server::DeltaService, ColumnDef, CommitInfo, CreateTableRequest,
    CreateTableResponse, GetTableInfoRequest, GetTableInfoResponse, HealthRequest, HealthResponse,
    HistoryRequest, HistoryResponse, OptimizeRequest, OptimizeResponse, ReadRequest, ReadResponse,
    VacuumRequest, VacuumResponse, WriteRequest, WriteResponse,
};

// ── helpers ──────────────────────────────────────────────────────────────────

/// Convert a proto ColumnDef into an Arrow Field (used for the write path).
fn proto_to_arrow_field(col: &ColumnDef) -> Field {
    let dt = match col.data_type.to_lowercase().as_str() {
        "string" | "str" => ArrowType::Utf8,
        "int32" | "integer" | "int" => ArrowType::Int32,
        "int64" | "long" => ArrowType::Int64,
        "float32" | "float" => ArrowType::Float32,
        "float64" | "double" => ArrowType::Float64,
        "boolean" | "bool" => ArrowType::Boolean,
        "timestamp" | "timestamptz" => {
            ArrowType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        "date" => ArrowType::Date32,
        _ => ArrowType::Utf8,
    };
    Field::new(&col.name, dt, col.nullable)
}

/// Convert a proto ColumnDef into a delta-rs kernel StructField (used for CreateTable).
fn proto_to_delta_field(col: &ColumnDef) -> StructField {
    let dt = match col.data_type.to_lowercase().as_str() {
        "string" | "str" => DeltaType::Primitive(PrimitiveType::String),
        "int32" | "integer" | "int" => DeltaType::Primitive(PrimitiveType::Integer),
        "int64" | "long" => DeltaType::Primitive(PrimitiveType::Long),
        "float32" | "float" => DeltaType::Primitive(PrimitiveType::Float),
        "float64" | "double" => DeltaType::Primitive(PrimitiveType::Double),
        "boolean" | "bool" => DeltaType::Primitive(PrimitiveType::Boolean),
        "timestamp" | "timestamptz" => DeltaType::Primitive(PrimitiveType::TimestampNtz),
        "date" => DeltaType::Primitive(PrimitiveType::Date),
        _ => DeltaType::Primitive(PrimitiveType::String),
    };
    StructField::new(col.name.clone(), dt, col.nullable)
}

/// Convert a delta-rs kernel StructField into a proto ColumnDef (used for GetTableInfo).
fn kernel_field_to_proto(field: &StructField) -> ColumnDef {
    let data_type = match field.data_type() {
        DeltaType::Primitive(PrimitiveType::String) => "string",
        DeltaType::Primitive(PrimitiveType::Integer) => "int32",
        DeltaType::Primitive(PrimitiveType::Long) => "int64",
        DeltaType::Primitive(PrimitiveType::Float) => "float32",
        DeltaType::Primitive(PrimitiveType::Double) => "float64",
        DeltaType::Primitive(PrimitiveType::Boolean) => "boolean",
        DeltaType::Primitive(PrimitiveType::Timestamp)
        | DeltaType::Primitive(PrimitiveType::TimestampNtz) => "timestamp",
        DeltaType::Primitive(PrimitiveType::Date) => "date",
        _ => "string",
    }
    .to_string();
    ColumnDef {
        name: field.name().to_string(),
        data_type,
        nullable: field.is_nullable(),
    }
}

fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}

/// Infer an Arrow schema by scanning all JSON objects for key names.
/// All columns are typed as Utf8 (string) when no explicit schema is provided.
fn infer_schema_from_json(rows: &[Value]) -> Schema {
    let mut keys: Vec<String> = Vec::new();
    for row in rows {
        if let Value::Object(map) = row {
            for k in map.keys() {
                if !keys.contains(k) {
                    keys.push(k.clone());
                }
            }
        }
    }
    let fields: Vec<Field> = keys
        .into_iter()
        .map(|k| Field::new(k, ArrowType::Utf8, true))
        .collect();
    Schema::new(fields)
}

// ── service ───────────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
pub struct DeltaServiceImpl;

#[tonic::async_trait]
impl DeltaService for DeltaServiceImpl {
    // ── Health ────────────────────────────────────────────────────────────────
    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            status: "ok".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        }))
    }

    // ── CreateTable ───────────────────────────────────────────────────────────
    async fn create_table(
        &self,
        request: Request<CreateTableRequest>,
    ) -> Result<Response<CreateTableResponse>, Status> {
        let req = request.into_inner();
        info!("create_table uri={}", req.table_uri);

        let columns: Vec<StructField> = req.schema.iter().map(proto_to_delta_field).collect();

        match DeltaOps::try_from_uri(&req.table_uri)
            .await
            .map_err(internal)?
            .create()
            .with_columns(columns)
            .with_partition_columns(req.partition_columns.clone())
            .await
        {
            Ok(_) => Ok(Response::new(CreateTableResponse {
                created: true,
                message: format!("table created at {}", req.table_uri),
            })),
            Err(e) => {
                let msg = e.to_string().to_lowercase();
                // Treat "already exists" as a non-error: the table is there.
                if msg.contains("already exists")
                    || msg.contains("table already")
                    || msg.contains("table version")
                {
                    info!("create_table: table already exists at {}", req.table_uri);
                    Ok(Response::new(CreateTableResponse {
                        created: false,
                        message: "table already exists".into(),
                    }))
                } else {
                    Err(internal(e))
                }
            }
        }
    }

    // ── Write ─────────────────────────────────────────────────────────────────
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let req = request.into_inner();
        info!("write uri={} mode={}", req.table_uri, req.mode);

        let save_mode = match req.mode.to_lowercase().as_str() {
            "overwrite" => SaveMode::Overwrite,
            _ => SaveMode::Append,
        };

        // Parse JSON array.
        let rows: Vec<Value> = serde_json::from_str(&req.json_data)
            .map_err(|e| Status::invalid_argument(format!("invalid json_data: {e}")))?;

        let num_rows = rows.len() as i64;
        if num_rows == 0 {
            return Ok(Response::new(WriteResponse {
                version: -1,
                rows_written: 0,
            }));
        }

        // Build Arrow schema — use provided columns or infer from JSON.
        let arrow_schema = if req.schema.is_empty() {
            infer_schema_from_json(&rows)
        } else {
            let fields: Vec<Field> = req.schema.iter().map(proto_to_arrow_field).collect();
            Schema::new(fields)
        };
        let schema_ref = Arc::new(arrow_schema);

        // Re-serialize as newline-delimited JSON so arrow-json can parse it.
        let ndjson: String = rows
            .iter()
            .filter_map(|r| serde_json::to_string(r).ok())
            .collect::<Vec<_>>()
            .join("\n");

        let reader = ReaderBuilder::new(schema_ref)
            .build(Cursor::new(ndjson.as_bytes()))
            .map_err(internal)?;

        let batches: Vec<_> = reader.collect::<Result<_, _>>().map_err(internal)?;

        if batches.is_empty() {
            return Ok(Response::new(WriteResponse {
                version: -1,
                rows_written: 0,
            }));
        }

        let table = DeltaOps::try_from_uri(&req.table_uri)
            .await
            .map_err(internal)?
            .write(batches)
            .with_save_mode(save_mode)
            .await
            .map_err(internal)?;

        Ok(Response::new(WriteResponse {
            version: table.version(),
            rows_written: num_rows,
        }))
    }

    // ── Read ──────────────────────────────────────────────────────────────────
    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        let req = request.into_inner();
        info!("read uri={}", req.table_uri);

        let table = if req.version.is_empty() {
            deltalake::open_table(&req.table_uri)
                .await
                .map_err(internal)?
        } else {
            let v: i64 = req
                .version
                .parse()
                .map_err(|_| Status::invalid_argument("version must be an integer"))?;
            deltalake::open_table_with_version(&req.table_uri, v)
                .await
                .map_err(internal)?
        };

        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(table)).map_err(internal)?;

        let sql = build_select_sql(&req.filter, req.limit);
        let df = ctx.sql(&sql).await.map_err(internal)?;
        let batches = df.collect().await.map_err(internal)?;

        // Convert batches → newline-delimited JSON, then wrap in array.
        let mut json_rows: Vec<String> = Vec::new();
        for batch in &batches {
            let mut buf = Vec::new();
            let mut writer = arrow_json::LineDelimitedWriter::new(&mut buf);
            writer.write(batch).map_err(internal)?;
            writer.finish().map_err(internal)?;
            let s = String::from_utf8(buf).map_err(internal)?;
            for line in s.lines() {
                if !line.trim().is_empty() {
                    json_rows.push(line.to_string());
                }
            }
        }

        let num_rows = json_rows.len() as i64;
        let json_data = format!("[{}]", json_rows.join(","));

        Ok(Response::new(ReadResponse {
            json_data,
            num_rows,
        }))
    }

    // ── GetTableInfo ──────────────────────────────────────────────────────────
    async fn get_table_info(
        &self,
        request: Request<GetTableInfoRequest>,
    ) -> Result<Response<GetTableInfoResponse>, Status> {
        let req = request.into_inner();
        info!("get_table_info uri={}", req.table_uri);

        let table = deltalake::open_table(&req.table_uri)
            .await
            .map_err(internal)?;

        let metadata = table.metadata().map_err(internal)?;

        // get_schema() returns &StructType from the delta kernel.
        let kernel_schema = table.get_schema().map_err(internal)?;
        let schema_fields: Vec<ColumnDef> =
            kernel_schema.fields().map(kernel_field_to_proto).collect();

        let partition_columns = metadata.partition_columns.clone();
        let num_files = table.get_files_count() as i64;
        let created_time = metadata
            .created_time
            .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_default();

        Ok(Response::new(GetTableInfoResponse {
            version: table.version(),
            schema: schema_fields,
            partition_columns,
            num_files,
            created_time,
        }))
    }

    // ── History ───────────────────────────────────────────────────────────────
    async fn history(
        &self,
        request: Request<HistoryRequest>,
    ) -> Result<Response<HistoryResponse>, Status> {
        let req = request.into_inner();
        info!("history uri={}", req.table_uri);

        let table = deltalake::open_table(&req.table_uri)
            .await
            .map_err(internal)?;

        let limit = if req.limit > 0 {
            Some(req.limit as usize)
        } else {
            None
        };

        let current_version = table.version();
        let commits_raw = table.history(limit).await.map_err(internal)?;

        // CommitInfo in deltalake 0.22 does not carry a version field.
        // Commits are returned newest-first, so we infer version by counting
        // backwards from the current table version.
        let commits: Vec<CommitInfo> = commits_raw
            .into_iter()
            .enumerate()
            .map(|(i, c)| CommitInfo {
                version: current_version - i as i64,
                timestamp: c
                    .timestamp
                    .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_default(),
                operation: c.operation.unwrap_or_default(),
                operation_parameters: c
                    .operation_parameters
                    .map(|p| serde_json::to_string(&p).unwrap_or_default())
                    .unwrap_or_default(),
            })
            .collect();

        Ok(Response::new(HistoryResponse { commits }))
    }

    // ── Vacuum ────────────────────────────────────────────────────────────────
    async fn vacuum(
        &self,
        request: Request<VacuumRequest>,
    ) -> Result<Response<VacuumResponse>, Status> {
        let req = request.into_inner();
        info!("vacuum uri={} dry_run={}", req.table_uri, req.dry_run);

        let table = deltalake::open_table(&req.table_uri)
            .await
            .map_err(internal)?;

        // snapshot() returns &DeltaTableState; VacuumBuilder needs an owned copy.
        let mut builder = VacuumBuilder::new(
            table.log_store(),
            table.snapshot().map_err(internal)?.clone(),
        )
        .with_dry_run(req.dry_run);

        if req.retention_hours > 0.0 {
            let duration = chrono::Duration::seconds((req.retention_hours * 3600.0) as i64);
            builder = builder.with_retention_period(duration);
        }

        let (_, metrics) = builder.await.map_err(internal)?;

        let deleted_files = metrics.files_deleted.clone();
        let num_deleted = deleted_files.len() as i64;

        if req.dry_run {
            warn!("vacuum dry_run: would delete {} files", num_deleted);
        }

        Ok(Response::new(VacuumResponse {
            deleted_files,
            num_deleted,
        }))
    }

    // ── Optimize ──────────────────────────────────────────────────────────────
    async fn optimize(
        &self,
        request: Request<OptimizeRequest>,
    ) -> Result<Response<OptimizeResponse>, Status> {
        let req = request.into_inner();
        let target_size = if req.target_size_bytes > 0 {
            req.target_size_bytes
        } else {
            256 * 1024 * 1024 // 256 MiB
        };
        info!(
            "optimize uri={} target_size={} partition_filter={:?}",
            req.table_uri, target_size, req.partition_filter
        );

        use deltalake::kernel::Action;
        use deltalake::operations::transaction::{CommitBuilder, CommitProperties};
        use deltalake::protocol::DeltaOperation;
        use deltalake::schema::partitions::PartitionValue;
        use deltalake::PartitionFilter;
        use deltalake::storage::object_store::path::Path as OsPath;

        let mut partition_filters: Vec<PartitionFilter> = Vec::new();
        if !req.partition_filter.is_empty() {
            if let Some((key, value)) = req.partition_filter.split_once('=') {
                partition_filters.push(PartitionFilter {
                    key: key.trim().to_string(),
                    value: PartitionValue::Equal(value.trim().to_string()),
                });
            } else {
                warn!(
                    "partition_filter {:?} is not in 'key=value' format — ignored",
                    req.partition_filter
                );
            }
        }

        let mut table = deltalake::open_table(&req.table_uri)
            .await
            .map_err(internal)?;

        // ── Repair DuckDB size=1 bug ─────────────────────────────────────────
        // DuckDB writes size=1 for every file in the delta log. delta-rs uses
        // this value to issue parquet range reads (bytes=0-0), which returns a
        // single byte and fails to parse as parquet.
        //
        // Fix: emit Add-only actions (no Remove) with corrected sizes.
        // Using Remove+Add in the same commit causes delta_kernel to drop the
        // re-Add (Remove wins), shrinking the table. A newer Add with a later
        // modification_time supersedes the old Add without needing a Remove.
        //
        // Recovery: also scan the previous table version to recover any files
        // dropped by a prior Remove+Add repair (they will have been "Removed"
        // from the current snapshot but still exist on S3).
        {
            let log_store = table.log_store().clone();
            let snapshot = table.snapshot().map_err(internal)?.clone();

            // Current active files
            let mut all_adds: Vec<_> = snapshot.file_actions().map_err(internal)?
                .into_iter().collect();

            // Recovery: if there's a recent FSCK commit that incorrectly removed
            // size=1 files via Remove+Add, load the version just before that FSCK
            // and recover any dropped size=1 files.  This is O(1) version loads
            // vs scanning all history.
            if table.version() > 0 {
                let history = table.history(Some(100)).await.unwrap_or_default();
                let mut seen_paths: std::collections::HashSet<String> =
                    all_adds.iter().map(|a| a.path.clone()).collect();
                let current_count = all_adds.len();
                for (idx, commit) in history.iter().enumerate() {
                    let fsck_version = table.version() - idx as i64;
                    if commit.operation.as_deref() == Some("FSCK") && fsck_version > 0 {
                        let pre_fsck_version = fsck_version - 1;
                        if let Ok(pre_fsck) =
                            deltalake::DeltaTableBuilder::from_uri(&req.table_uri)
                                .with_version(pre_fsck_version)
                                .load()
                                .await
                        {
                            if let Ok(pre_snap) = pre_fsck.snapshot() {
                                if let Ok(pre_adds) = pre_snap.file_actions() {
                                    if pre_adds.len() > current_count {
                                        warn!(
                                            "FSCK at v{} dropped files; loading v{} ({} files) to recover",
                                            fsck_version, pre_fsck_version, pre_adds.len()
                                        );
                                        for add in pre_adds {
                                            if !seen_paths.contains(&add.path) && add.size < 512 {
                                                seen_paths.insert(add.path.clone());
                                                all_adds.push(add);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        break; // Only handle the most recent FSCK
                    }
                }
            }

            // 512 bytes is the minimum threshold: no valid parquet file is
            // smaller. This catches size=1 (DuckDB bug) as well as size=2..511
            // (corrupted metadata from a bad HEAD response or DuckDB variant).
            let bad_adds: Vec<_> = all_adds
                .into_iter()
                .filter(|a| {
                    if a.size >= 512 {
                        return false;
                    }
                    if partition_filters.is_empty() {
                        return true;
                    }
                    partition_filters.iter().all(|pf| match &pf.value {
                        PartitionValue::Equal(expected) => a
                            .partition_values
                            .get(&pf.key)
                            .and_then(|v| v.as_ref())
                            .map(|v| v == expected)
                            .unwrap_or(false),
                        _ => true,
                    })
                })
                .collect();

            if !bad_adds.is_empty() {
                warn!(
                    "found {} files with size<512 (DuckDB metadata bug), \
                     repairing sizes before optimize",
                    bad_adds.len()
                );
                let object_store = log_store.object_store();
                let mut repair_actions: Vec<Action> = Vec::new();

                for add in bad_adds {
                    let path = OsPath::from(add.path.as_str());
                    let meta = object_store.head(&path).await.map_err(|e| {
                        Status::internal(format!("HEAD {} failed: {e}", add.path))
                    })?;
                    let real_size = meta.size as i64;
                    // Guard: if the HEAD response is also too small, the
                    // network may have returned a malformed response. Skip
                    // this file rather than committing a still-wrong size.
                    if real_size < 512 {
                        warn!(
                            "HEAD returned suspicious size {} for {}, skipping",
                            real_size, add.path
                        );
                        continue;
                    }

                    let mut corrected = add;
                    corrected.size = real_size;
                    corrected.modification_time = Utc::now().timestamp_millis();
                    corrected.data_change = false;
                    repair_actions.push(Action::Add(corrected));
                }

                CommitBuilder::from(CommitProperties::default())
                    .with_actions(repair_actions)
                    .build(
                        Some(&snapshot),
                        log_store,
                        DeltaOperation::FileSystemCheck {},
                    )
                    .await
                    .map_err(internal)?;

                table.load().await.map_err(internal)?;
            }
        }

        // ── Run optimize with corrected metadata ─────────────────────────────
        let base = DeltaOps(table).optimize().with_target_size(target_size);
        let base = if !partition_filters.is_empty() {
            base.with_filters(&partition_filters)
        } else {
            base
        };
        let builder = if !req.z_order_columns.is_empty() {
            base.with_z_order_columns(req.z_order_columns.clone())
        } else {
            base
        };

        let (_, metrics) = builder.await.map_err(internal)?;

        Ok(Response::new(OptimizeResponse {
            files_added: metrics.num_files_added as i64,
            files_removed: metrics.num_files_removed as i64,
            partitions_optimized: metrics.partitions_optimized as i64,
        }))
    }
}

// ── SQL builder ───────────────────────────────────────────────────────────────

fn build_select_sql(filter: &str, limit: i64) -> String {
    let mut sql = "SELECT * FROM t".to_string();
    if !filter.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(filter);
    }
    if limit > 0 {
        sql.push_str(&format!(" LIMIT {limit}"));
    }
    sql
}
