use std::collections::HashMap;
use std::io::Cursor;
use std::ops::Range;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arrow::datatypes::{DataType as ArrowType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_json::ReaderBuilder;
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use deltalake::datafusion::prelude::SessionContext;
use deltalake::kernel::{DataType as DeltaType, PrimitiveType, StructField, Transaction};
use deltalake::operations::transaction::CommitProperties;
use deltalake::operations::vacuum::VacuumBuilder;
use deltalake::protocol::SaveMode;
use deltalake::storage::object_store::{path::Path as OsPath, ObjectMeta, ObjectStore};
use deltalake::{DeltaOps, DeltaTable};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::delta_proto::{
    delta_service_server::DeltaService, ColumnDef, CommitInfo, CreateTableRequest,
    CreateTableResponse, GetTableInfoRequest, GetTableInfoResponse, HealthRequest, HealthResponse,
    HistoryRequest, HistoryResponse, OptimizeRequest, OptimizeResponse, ReadRequest, ReadResponse,
    RewriteCheckpointMultipartRequest, RewriteCheckpointMultipartResponse, VacuumRequest,
    VacuumResponse, WriteRequest, WriteResponse,
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

fn is_retryable_storage_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    if lower.contains("copy-if-not-exists") || lower.contains("no files in log segment") {
        return false;
    }
    [
        "error decoding response body",
        "generic s3 error",
        "failed to parse parquet",
        "failed to read delta log object",
        "timeout",
        "timed out",
        "deadline",
        "connection",
        "connection reset",
        "broken pipe",
        "temporarily unavailable",
        "slow down",
        "503",
        "500",
    ]
    .iter()
    .any(|fragment| lower.contains(fragment))
}

fn delta_status(phase: &str, table_uri: &str, message: &str) -> Status {
    let retryable = is_retryable_storage_error(message);
    Status::internal(format!(
        "delta_error phase={phase} retryable={retryable} table_uri={table_uri} message={message}"
    ))
}

fn bytes_preview(bytes: &[u8]) -> String {
    let hex = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join(" ");
    let ascii: String = bytes
        .iter()
        .map(|byte| {
            if byte.is_ascii_graphic() || *byte == b' ' {
                *byte as char
            } else {
                '.'
            }
        })
        .collect();
    format!("hex={hex} ascii={ascii}")
}

async fn read_object_range_diag(
    object_store: &Arc<dyn ObjectStore>,
    path: &OsPath,
    range: Range<usize>,
    label: &str,
) {
    match tokio::time::timeout(
        Duration::from_secs(30),
        object_store.get_range(path, range.clone()),
    )
    .await
    {
        Ok(Ok(bytes)) => warn!(
            object_path = path.as_ref(),
            range_start = range.start,
            range_end = range.end,
            len = bytes.len(),
            preview = %bytes_preview(&bytes),
            "delta diag object range read ok: {label}"
        ),
        Ok(Err(error)) => warn!(
            object_path = path.as_ref(),
            range_start = range.start,
            range_end = range.end,
            error = %error,
            "delta diag object range read failed: {label}"
        ),
        Err(_) => warn!(
            object_path = path.as_ref(),
            range_start = range.start,
            range_end = range.end,
            "delta diag object range read timed out: {label}"
        ),
    }
}

async fn log_object_read_diag(
    object_store: &Arc<dyn ObjectStore>,
    path: &OsPath,
    expected_size: Option<usize>,
) {
    warn!(
        object_path = path.as_ref(),
        expected_size, "delta diag object check start"
    );

    let meta = match tokio::time::timeout(Duration::from_secs(30), object_store.head(path)).await {
        Ok(Ok(meta)) => {
            warn!(
                object_path = path.as_ref(),
                size = meta.size,
                last_modified = %meta.last_modified,
                e_tag = ?meta.e_tag,
                version = ?meta.version,
                "delta diag object head ok"
            );
            Some(meta)
        }
        Ok(Err(error)) => {
            warn!(
                object_path = path.as_ref(),
                error = %error,
                "delta diag object head failed"
            );
            None
        }
        Err(_) => {
            warn!(
                object_path = path.as_ref(),
                "delta diag object head timed out"
            );
            None
        }
    };

    let size = meta.as_ref().map(|meta| meta.size).or(expected_size);
    if let Some(size) = size {
        if size >= 4 {
            read_object_range_diag(object_store, path, 0..4, "first bytes").await;
        }
        if size >= 8 {
            read_object_range_diag(object_store, path, size - 8..size, "last bytes").await;
        }
    }

    let get_result =
        match tokio::time::timeout(Duration::from_secs(30), object_store.get(path)).await {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => {
                warn!(
                    object_path = path.as_ref(),
                    error = %error,
                    "delta diag object get failed"
                );
                return;
            }
            Err(_) => {
                warn!(
                    object_path = path.as_ref(),
                    "delta diag object get timed out"
                );
                return;
            }
        };

    match tokio::time::timeout(Duration::from_secs(120), get_result.bytes()).await {
        Ok(Ok(bytes)) => warn!(
            object_path = path.as_ref(),
            len = bytes.len(),
            "delta diag object full body read ok"
        ),
        Ok(Err(error)) => warn!(
            object_path = path.as_ref(),
            error = %error,
            "delta diag object full body read failed"
        ),
        Err(_) => warn!(
            object_path = path.as_ref(),
            "delta diag object full body read timed out"
        ),
    }
}

async fn log_table_load_diag(table_uri: &str, phase: &str, load_error: &str) {
    warn!(
        uri = table_uri,
        phase,
        error = load_error,
        "delta table load failed; collecting object diagnostics"
    );

    let log_store = match table_builder(table_uri).build_storage() {
        Ok(log_store) => log_store,
        Err(error) => {
            warn!(
                uri = table_uri,
                phase,
                error = %error,
                "delta diag build_storage failed"
            );
            return;
        }
    };

    let object_store = log_store.object_store();
    let last_checkpoint_path = log_store.log_path().child("_last_checkpoint");
    let last_checkpoint_body = match tokio::time::timeout(
        Duration::from_secs(30),
        object_store.get(&last_checkpoint_path),
    )
    .await
    {
        Ok(Ok(result)) => match tokio::time::timeout(Duration::from_secs(30), result.bytes()).await
        {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(error)) => {
                warn!(
                    object_path = last_checkpoint_path.as_ref(),
                    error = %error,
                    "delta diag _last_checkpoint body read failed"
                );
                return;
            }
            Err(_) => {
                warn!(
                    object_path = last_checkpoint_path.as_ref(),
                    "delta diag _last_checkpoint body read timed out"
                );
                return;
            }
        },
        Ok(Err(error)) => {
            warn!(
                object_path = last_checkpoint_path.as_ref(),
                error = %error,
                "delta diag _last_checkpoint get failed"
            );
            return;
        }
        Err(_) => {
            warn!(
                object_path = last_checkpoint_path.as_ref(),
                "delta diag _last_checkpoint get timed out"
            );
            return;
        }
    };

    let last_checkpoint_text = String::from_utf8_lossy(&last_checkpoint_body);
    warn!(
        object_path = last_checkpoint_path.as_ref(),
        body = %last_checkpoint_text,
        "delta diag _last_checkpoint read ok"
    );

    let checkpoint_json: Value = match serde_json::from_slice(&last_checkpoint_body) {
        Ok(value) => value,
        Err(error) => {
            warn!(
                object_path = last_checkpoint_path.as_ref(),
                error = %error,
                "delta diag _last_checkpoint json parse failed"
            );
            return;
        }
    };

    let Some(version) = checkpoint_json.get("version").and_then(Value::as_i64) else {
        warn!(
            object_path = last_checkpoint_path.as_ref(),
            "delta diag _last_checkpoint missing version"
        );
        return;
    };

    let expected_size = checkpoint_json
        .get("sizeInBytes")
        .and_then(Value::as_u64)
        .and_then(|size| usize::try_from(size).ok());
    let parts = checkpoint_json
        .get("parts")
        .and_then(Value::as_u64)
        .unwrap_or(1);

    if parts <= 1 {
        let checkpoint_path = log_store
            .log_path()
            .child(format!("{version:020}.checkpoint.parquet"));
        log_object_read_diag(&object_store, &checkpoint_path, expected_size).await;
        return;
    }

    for part in 1..=parts {
        let checkpoint_path = log_store.log_path().child(format!(
            "{version:020}.checkpoint.{part:010}.{parts:010}.parquet"
        ));
        log_object_read_diag(&object_store, &checkpoint_path, None).await;
    }
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

fn storage_options() -> &'static HashMap<String, String> {
    static OPTS: OnceLock<HashMap<String, String>> = OnceLock::new();
    OPTS.get_or_init(|| {
        let mut m = HashMap::new();
        if let Ok(v) = std::env::var("AWS_CONDITIONAL_PUT") {
            m.insert("conditional_put".to_string(), v);
        }
        if let Ok(v) = std::env::var("AWS_S3_ALLOW_UNSAFE_RENAME") {
            m.insert("AWS_S3_ALLOW_UNSAFE_RENAME".to_string(), v);
        }
        m
    })
}

fn configured_log_buffer_size() -> Option<usize> {
    static LOG_BUFFER_SIZE: OnceLock<Option<usize>> = OnceLock::new();
    *LOG_BUFFER_SIZE.get_or_init(|| match std::env::var("DELTA_LOG_BUFFER_SIZE") {
        Ok(value) => match value.parse::<usize>() {
            Ok(parsed) if parsed > 0 => Some(parsed),
            _ => {
                warn!(
                    value,
                    "DELTA_LOG_BUFFER_SIZE must be a positive integer; using delta-rs default"
                );
                None
            }
        },
        Err(_) => None,
    })
}

fn table_load_max_retries() -> usize {
    static RETRIES: OnceLock<usize> = OnceLock::new();
    *RETRIES.get_or_init(|| {
        std::env::var("DELTA_TABLE_LOAD_MAX_RETRIES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(3)
    })
}

fn table_load_retry_backoff_ms(attempt: usize) -> u64 {
    let base = std::env::var("DELTA_TABLE_LOAD_RETRY_BACKOFF_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(500);
    let capped_attempt = attempt.min(5) as u32;
    base.saturating_mul(2_u64.saturating_pow(capped_attempt))
}

fn table_builder(table_uri: &str) -> deltalake::DeltaTableBuilder {
    let builder = deltalake::DeltaTableBuilder::from_uri(table_uri)
        .with_storage_options(storage_options().clone());
    match configured_log_buffer_size() {
        Some(size) => match builder.with_log_buffer_size(size) {
            Ok(builder) => builder,
            Err(error) => {
                warn!(
                    uri = table_uri,
                    size,
                    error = %error,
                    "invalid DELTA_LOG_BUFFER_SIZE; using delta-rs default for this table load"
                );
                deltalake::DeltaTableBuilder::from_uri(table_uri)
                    .with_storage_options(storage_options().clone())
            }
        },
        None => builder,
    }
}

fn default_checkpoint_part_size_bytes() -> usize {
    static PART_SIZE: OnceLock<usize> = OnceLock::new();
    *PART_SIZE.get_or_init(|| {
        std::env::var("DELTA_CHECKPOINT_TARGET_PART_SIZE_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(32 * 1024 * 1024)
    })
}

fn object_read_chunk_size_bytes() -> usize {
    static CHUNK_SIZE: OnceLock<usize> = OnceLock::new();
    *CHUNK_SIZE.get_or_init(|| {
        std::env::var("DELTA_OBJECT_READ_CHUNK_SIZE_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(8 * 1024 * 1024)
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LastCheckpointMetadata {
    version: i64,
    size: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    parts: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    size_in_bytes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    num_of_add_files: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checkpoint_schema: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checksum: Option<String>,
}

struct CheckpointSource {
    path: OsPath,
    meta: ObjectMeta,
    bytes: Bytes,
}

struct CheckpointPart {
    path: OsPath,
    bytes: Bytes,
    rows: usize,
}

async fn get_range_with_retry(
    object_store: &Arc<dyn ObjectStore>,
    path: &OsPath,
    range: Range<usize>,
) -> Result<Bytes, Status> {
    let mut last_error = None;
    for attempt in 1..=4 {
        match object_store.get_range(path, range.clone()).await {
            Ok(bytes) => return Ok(bytes),
            Err(error) => {
                last_error = Some(error.to_string());
                warn!(
                    object_path = path.as_ref(),
                    range_start = range.start,
                    range_end = range.end,
                    attempt,
                    error = %last_error.as_deref().unwrap_or_default(),
                    "checkpoint range read failed; retrying"
                );
                tokio::time::sleep(Duration::from_millis(250 * attempt)).await;
            }
        }
    }

    Err(Status::internal(format!(
        "read range {}..{} from {} failed after retries: {}",
        range.start,
        range.end,
        path.as_ref(),
        last_error.unwrap_or_else(|| "unknown error".to_string())
    )))
}

async fn read_object_chunked(
    object_store: &Arc<dyn ObjectStore>,
    path: &OsPath,
    size: usize,
) -> Result<Bytes, Status> {
    let chunk_size = object_read_chunk_size_bytes();
    let mut data = Vec::with_capacity(size);
    let mut start = 0;
    while start < size {
        let end = usize::min(start + chunk_size, size);
        let bytes = get_range_with_retry(object_store, path, start..end).await?;
        if bytes.len() != end - start {
            return Err(Status::internal(format!(
                "read range {}..{} from {} returned {} bytes",
                start,
                end,
                path.as_ref(),
                bytes.len()
            )));
        }
        data.extend_from_slice(&bytes);
        start = end;
    }
    Ok(Bytes::from(data))
}

fn checkpoint_source_paths(version: i64, parts: Option<i32>) -> Result<Vec<OsPath>, Status> {
    let part_count = parts.unwrap_or(1);
    if part_count <= 1 {
        return Ok(vec![OsPath::from(format!(
            "_delta_log/{version:020}.checkpoint.parquet"
        ))]);
    }

    let part_count = usize::try_from(part_count)
        .map_err(|_| Status::internal("checkpoint parts value is invalid"))?;
    Ok((1..=part_count)
        .map(|part| {
            OsPath::from(format!(
                "_delta_log/{version:020}.checkpoint.{part:010}.{part_count:010}.parquet"
            ))
        })
        .collect())
}

fn checkpoint_part_path(version: i64, part: usize, parts: usize) -> OsPath {
    OsPath::from(format!(
        "_delta_log/{version:020}.checkpoint.{part:010}.{parts:010}.parquet"
    ))
}

fn checkpoint_backup_prefix(version: i64) -> OsPath {
    let timestamp = Utc::now().format("%Y%m%dT%H%M%SZ");
    OsPath::from(format!("_checkpoint_backup/{timestamp}-v{version}"))
}

fn compute_target_parts(
    request_target_parts: i32,
    request_target_part_size_bytes: i64,
    source_size: usize,
    rows: i64,
) -> Result<usize, Status> {
    let mut parts = if request_target_parts > 0 {
        usize::try_from(request_target_parts)
            .map_err(|_| Status::invalid_argument("target_parts is invalid"))?
    } else {
        let target_size = if request_target_part_size_bytes > 0 {
            usize::try_from(request_target_part_size_bytes).map_err(|_| {
                Status::invalid_argument("target_part_size_bytes does not fit in usize")
            })?
        } else {
            default_checkpoint_part_size_bytes()
        };
        usize::max(1, source_size.div_ceil(target_size))
    };

    let rows = usize::try_from(rows)
        .map_err(|_| Status::internal("checkpoint row count does not fit in usize"))?;
    if rows == 0 {
        return Err(Status::internal("checkpoint has no rows"));
    }
    parts = usize::min(parts, rows);
    if parts == 0 {
        return Err(Status::invalid_argument("target_parts must be positive"));
    }
    Ok(parts)
}

fn validate_source_schema(
    schema: &mut Option<SchemaRef>,
    builder: &ParquetRecordBatchReaderBuilder<Bytes>,
) -> Result<(), Status> {
    match schema {
        Some(existing) if existing.as_ref() != builder.schema().as_ref() => Err(Status::internal(
            "checkpoint parquet parts have different Arrow schemas",
        )),
        Some(_) => Ok(()),
        None => {
            *schema = Some(builder.schema().clone());
            Ok(())
        }
    }
}

fn write_checkpoint_parts(
    sources: &[CheckpointSource],
    version: i64,
    target_parts: usize,
) -> Result<(Vec<CheckpointPart>, i64), Status> {
    let mut schema = None;
    let mut total_rows: i64 = 0;
    for source in sources {
        let builder = ParquetRecordBatchReaderBuilder::try_new(source.bytes.clone())
            .map_err(|error| Status::internal(format!("read checkpoint metadata: {error}")))?;
        validate_source_schema(&mut schema, &builder)?;
        total_rows += builder.metadata().file_metadata().num_rows();
    }
    let schema = schema.ok_or_else(|| Status::internal("checkpoint has no parquet sources"))?;
    let total_rows_usize = usize::try_from(total_rows)
        .map_err(|_| Status::internal("checkpoint row count does not fit in usize"))?;
    if target_parts > total_rows_usize {
        return Err(Status::invalid_argument(
            "target_parts cannot be greater than checkpoint row count",
        ));
    }

    let base_rows = total_rows_usize / target_parts;
    let extra_rows = total_rows_usize % target_parts;
    let target_rows = |part_index: usize| base_rows + usize::from(part_index < extra_rows);
    let write_props = || {
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build()
    };

    let mut parts = Vec::with_capacity(target_parts);
    let mut current_part = 0;
    let mut current_rows = 0usize;
    let mut writer = Some(
        ArrowWriter::try_new(Vec::new(), schema.clone(), Some(write_props()))
            .map_err(|error| Status::internal(format!("create checkpoint writer: {error}")))?,
    );

    for source in sources {
        let reader = ParquetRecordBatchReaderBuilder::try_new(source.bytes.clone())
            .map_err(|error| Status::internal(format!("open checkpoint reader: {error}")))?
            .with_batch_size(1024)
            .build()
            .map_err(|error| Status::internal(format!("build checkpoint reader: {error}")))?;

        for batch in reader {
            let batch: RecordBatch = batch
                .map_err(|error| Status::internal(format!("read checkpoint batch: {error}")))?;
            let mut offset = 0;
            while offset < batch.num_rows() {
                let remaining_batch = batch.num_rows() - offset;
                let remaining_part = target_rows(current_part) - current_rows;
                let take = usize::min(remaining_batch, remaining_part);
                let sliced = batch.slice(offset, take);
                writer
                    .as_mut()
                    .ok_or_else(|| Status::internal("checkpoint part writer is not open"))?
                    .write(&sliced)
                    .map_err(|error| Status::internal(format!("write checkpoint part: {error}")))?;
                offset += take;
                current_rows += take;

                if current_rows == target_rows(current_part) {
                    let bytes = writer
                        .take()
                        .ok_or_else(|| Status::internal("checkpoint part writer is not open"))?
                        .into_inner()
                        .map_err(|error| {
                            Status::internal(format!("finish checkpoint part: {error}"))
                        })?;
                    let part_number = current_part + 1;
                    parts.push(CheckpointPart {
                        path: checkpoint_part_path(version, part_number, target_parts),
                        bytes: Bytes::from(bytes),
                        rows: current_rows,
                    });
                    current_part += 1;
                    current_rows = 0;
                    if current_part < target_parts {
                        writer = Some(
                            ArrowWriter::try_new(Vec::new(), schema.clone(), Some(write_props()))
                                .map_err(|error| {
                                Status::internal(format!("create checkpoint writer: {error}"))
                            })?,
                        );
                    }
                }
            }
        }
    }

    if parts.len() != target_parts {
        return Err(Status::internal(format!(
            "wrote {} checkpoint parts, expected {}",
            parts.len(),
            target_parts
        )));
    }
    Ok((parts, total_rows))
}

pub struct DeltaServiceImpl {
    tables: Arc<RwLock<HashMap<String, DeltaTable>>>,
}

async fn load_cached_table_with_retry(
    table_uri: &str,
    table: &mut DeltaTable,
) -> Result<(), String> {
    let max_retries = table_load_max_retries();
    for attempt in 0..=max_retries {
        match table.load().await {
            Ok(()) => return Ok(()),
            Err(error) => {
                let message = error.to_string();
                if attempt >= max_retries || !is_retryable_storage_error(&message) {
                    return Err(message);
                }
                let backoff_ms = table_load_retry_backoff_ms(attempt);
                warn!(
                    uri = table_uri,
                    phase = "cached_load",
                    attempt = attempt + 1,
                    max_retries,
                    backoff_ms,
                    error = %message,
                    "delta table load failed, retrying"
                );
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }
        }
    }
    Err("delta table load retry loop exhausted".to_string())
}

async fn load_fresh_table_with_retry(table_uri: &str) -> Result<DeltaTable, String> {
    let max_retries = table_load_max_retries();
    for attempt in 0..=max_retries {
        match table_builder(table_uri).load().await {
            Ok(table) => return Ok(table),
            Err(error) => {
                let message = error.to_string();
                if attempt >= max_retries || !is_retryable_storage_error(&message) {
                    return Err(message);
                }
                let backoff_ms = table_load_retry_backoff_ms(attempt);
                warn!(
                    uri = table_uri,
                    phase = "cold_open",
                    attempt = attempt + 1,
                    max_retries,
                    backoff_ms,
                    error = %message,
                    "delta table cold open failed, retrying"
                );
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }
        }
    }
    Err("delta table cold-open retry loop exhausted".to_string())
}

impl DeltaServiceImpl {
    pub fn new() -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get a cached table or open it fresh. On cache hit, only loads new commits
    /// since the cached version (incremental refresh instead of full checkpoint download).
    async fn get_or_open_table(&self, table_uri: &str) -> Result<DeltaTable, Status> {
        {
            let mut cache = self.tables.write().await;
            if let Some(table) = cache.get_mut(table_uri) {
                // Incremental load — only reads commits newer than cached version
                if let Err(error) = load_cached_table_with_retry(table_uri, table).await {
                    log_table_load_diag(table_uri, "cached_load", &error).await;
                    return Err(delta_status("cached_load", table_uri, &error));
                }
                return Ok(table.clone());
            }
        }

        // Cold open — full checkpoint download (happens once per table URI)
        let table = match load_fresh_table_with_retry(table_uri).await {
            Ok(table) => table,
            Err(error) => {
                log_table_load_diag(table_uri, "cold_open", &error).await;
                return Err(delta_status("cold_open", table_uri, &error));
            }
        };
        let mut cache = self.tables.write().await;
        cache.insert(table_uri.to_string(), table.clone());
        Ok(table)
    }

    /// Evict a table from cache (used after operations that heavily mutate state
    /// like optimize, so the next open gets a clean snapshot).
    async fn evict_table(&self, table_uri: &str) {
        let mut cache = self.tables.write().await;
        cache.remove(table_uri);
    }

    /// Update the cached table after a successful write.
    async fn update_cached_table(&self, table_uri: &str, table: DeltaTable) {
        let mut cache = self.tables.write().await;
        cache.insert(table_uri.to_string(), table);
    }
}

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

        match DeltaOps::try_from_uri_with_storage_options(&req.table_uri, storage_options().clone())
            .await
            .map_err(internal)?
            .create()
            .with_columns(columns)
            .with_partition_columns(req.partition_columns.clone())
            .await
        {
            Ok(_) => {
                self.evict_table(&req.table_uri).await;
                Ok(Response::new(CreateTableResponse {
                    created: true,
                    message: format!("table created at {}", req.table_uri),
                }))
            }
            Err(e) => {
                let msg = e.to_string().to_lowercase();
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
        info!(
            "write uri={} mode={} batch_id={}",
            req.table_uri, req.mode, req.batch_id
        );

        let is_overwrite = req.mode.eq_ignore_ascii_case("overwrite");
        let save_mode = if is_overwrite {
            SaveMode::Overwrite
        } else {
            SaveMode::Append
        };

        // Parse JSON array.
        let rows: Vec<Value> = serde_json::from_str(&req.json_data)
            .map_err(|e| Status::invalid_argument(format!("invalid json_data: {e}")))?;

        let num_rows = rows.len() as i64;
        if num_rows == 0 {
            return Ok(Response::new(WriteResponse {
                version: -1,
                rows_written: 0,
                already_committed: false,
                batch_id: req.batch_id,
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
                already_committed: false,
                batch_id: req.batch_id,
            }));
        }

        // Use cached table state — only reads new commits since last call. A
        // first overwrite is allowed to create a new table from an empty URI.
        let table = match self.get_or_open_table(&req.table_uri).await {
            Ok(table) => Some(table),
            Err(status) if is_overwrite && status.message().contains("Not a Delta table") => None,
            Err(status) if is_overwrite && status.message().contains("no log files") => None,
            Err(status) => return Err(status),
        };
        let app_transaction_id = req.app_transaction_id.trim().to_string();
        let app_transaction_version = req.app_transaction_version;
        if !app_transaction_id.is_empty() {
            if app_transaction_version <= 0 {
                return Err(Status::invalid_argument(
                    "app_transaction_version must be positive when app_transaction_id is set",
                ));
            }
            if let Some(table) = table.as_ref() {
                if let Some(existing) = table.get_app_transaction_version().get(&app_transaction_id)
                {
                    if existing.version >= app_transaction_version {
                        info!(
                            uri = req.table_uri,
                            batch_id = req.batch_id,
                            app_transaction_id,
                            app_transaction_version,
                            existing_version = existing.version,
                            "write skipped because application transaction is already committed"
                        );
                        return Ok(Response::new(WriteResponse {
                            version: table.version(),
                            rows_written: 0,
                            already_committed: true,
                            batch_id: req.batch_id,
                        }));
                    }
                }
            }
        }

        let ops = if let Some(table) = table {
            DeltaOps(table)
        } else {
            DeltaOps::try_from_uri_with_storage_options(&req.table_uri, storage_options().clone())
                .await
                .map_err(|error| {
                    let message = error.to_string();
                    delta_status("create_writer", &req.table_uri, &message)
                })?
        };

        let mut write = ops.write(batches).with_save_mode(save_mode);

        let mut metadata = HashMap::new();
        if !req.batch_id.is_empty() {
            metadata.insert("batchId".to_string(), Value::String(req.batch_id.clone()));
        }
        if !app_transaction_id.is_empty() {
            metadata.insert(
                "appTransactionId".to_string(),
                Value::String(app_transaction_id.clone()),
            );
            metadata.insert(
                "appTransactionVersion".to_string(),
                Value::Number(app_transaction_version.into()),
            );
        }
        if !metadata.is_empty() || !app_transaction_id.is_empty() {
            let mut commit_properties = CommitProperties::default().with_metadata(metadata);
            if !app_transaction_id.is_empty() {
                commit_properties = commit_properties.with_application_transaction(
                    Transaction::new(app_transaction_id, app_transaction_version),
                );
            }
            write = write.with_commit_properties(commit_properties);
        }

        let table = write.await.map_err(|error| {
            let message = error.to_string();
            delta_status("commit", &req.table_uri, &message)
        })?;

        let version = table.version();
        self.update_cached_table(&req.table_uri, table).await;

        Ok(Response::new(WriteResponse {
            version,
            rows_written: num_rows,
            already_committed: false,
            batch_id: req.batch_id,
        }))
    }

    // ── Read ──────────────────────────────────────────────────────────────────
    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        let req = request.into_inner();
        info!("read uri={}", req.table_uri);

        let table = if req.version.is_empty() {
            self.get_or_open_table(&req.table_uri).await?
        } else {
            let v: i64 = req
                .version
                .parse()
                .map_err(|_| Status::invalid_argument("version must be an integer"))?;
            table_builder(&req.table_uri)
                .with_version(v)
                .load()
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

        let table = self.get_or_open_table(&req.table_uri).await?;

        let metadata = table.metadata().map_err(internal)?;

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

        let table = self.get_or_open_table(&req.table_uri).await?;

        let limit = if req.limit > 0 {
            Some(req.limit as usize)
        } else {
            None
        };

        let current_version = table.version();
        let commits_raw = table.history(limit).await.map_err(internal)?;

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

    // ── Checkpoint Maintenance ───────────────────────────────────────────────
    async fn rewrite_checkpoint_multipart(
        &self,
        request: Request<RewriteCheckpointMultipartRequest>,
    ) -> Result<Response<RewriteCheckpointMultipartResponse>, Status> {
        let req = request.into_inner();
        info!(
            "rewrite_checkpoint_multipart uri={} target_part_size_bytes={} target_parts={} dry_run={}",
            req.table_uri, req.target_part_size_bytes, req.target_parts, req.dry_run
        );

        let log_store = table_builder(&req.table_uri)
            .build_storage()
            .map_err(internal)?;
        let object_store = log_store.object_store();
        let last_checkpoint_path = log_store.log_path().child("_last_checkpoint");
        let last_checkpoint_body = object_store
            .get(&last_checkpoint_path)
            .await
            .map_err(|error| {
                Status::internal(format!(
                    "read {} failed: {error}",
                    last_checkpoint_path.as_ref()
                ))
            })?
            .bytes()
            .await
            .map_err(|error| {
                Status::internal(format!(
                    "read {} body failed: {error}",
                    last_checkpoint_path.as_ref()
                ))
            })?;
        let mut checkpoint: LastCheckpointMetadata = serde_json::from_slice(&last_checkpoint_body)
            .map_err(|error| Status::internal(format!("parse _last_checkpoint failed: {error}")))?;

        let source_paths = checkpoint_source_paths(checkpoint.version, checkpoint.parts)?;
        let mut source_metas = Vec::with_capacity(source_paths.len());
        let mut source_size = 0usize;
        for path in &source_paths {
            let meta = object_store.head(path).await.map_err(|error| {
                Status::internal(format!("HEAD {} failed: {error}", path.as_ref()))
            })?;
            source_size += meta.size;
            source_metas.push(meta);
        }

        let target_parts = compute_target_parts(
            req.target_parts,
            req.target_part_size_bytes,
            source_size,
            checkpoint.size,
        )?;
        let source_parts = source_paths.len();
        if source_parts == target_parts && source_parts > 1 {
            return Ok(Response::new(RewriteCheckpointMultipartResponse {
                version: checkpoint.version,
                source_parts: source_parts as i32,
                target_parts: target_parts as i32,
                source_size_bytes: source_size as i64,
                target_size_bytes: source_size as i64,
                max_part_size_bytes: source_metas.iter().map(|meta| meta.size).max().unwrap_or(0)
                    as i64,
                rows: checkpoint.size,
                rewritten: false,
                backup_prefix: String::new(),
                checkpoint_files: source_paths
                    .iter()
                    .map(|path| path.as_ref().to_string())
                    .collect(),
                message: "latest checkpoint already has the requested part count".into(),
            }));
        }
        if target_parts <= 1 {
            return Ok(Response::new(RewriteCheckpointMultipartResponse {
                version: checkpoint.version,
                source_parts: source_parts as i32,
                target_parts: target_parts as i32,
                source_size_bytes: source_size as i64,
                target_size_bytes: source_size as i64,
                max_part_size_bytes: source_metas.iter().map(|meta| meta.size).max().unwrap_or(0)
                    as i64,
                rows: checkpoint.size,
                rewritten: false,
                backup_prefix: String::new(),
                checkpoint_files: source_paths
                    .iter()
                    .map(|path| path.as_ref().to_string())
                    .collect(),
                message: "target checkpoint would not be multipart".into(),
            }));
        }

        if req.dry_run {
            return Ok(Response::new(RewriteCheckpointMultipartResponse {
                version: checkpoint.version,
                source_parts: source_parts as i32,
                target_parts: target_parts as i32,
                source_size_bytes: source_size as i64,
                target_size_bytes: 0,
                max_part_size_bytes: 0,
                rows: checkpoint.size,
                rewritten: false,
                backup_prefix: String::new(),
                checkpoint_files: Vec::new(),
                message: "dry run: checkpoint rewrite plan computed".into(),
            }));
        }

        let backup_prefix = checkpoint_backup_prefix(checkpoint.version);
        object_store
            .put(
                &backup_prefix.child("_last_checkpoint"),
                last_checkpoint_body.clone().into(),
            )
            .await
            .map_err(|error| {
                Status::internal(format!("backup _last_checkpoint failed: {error}"))
            })?;

        for path in &source_paths {
            let name = path
                .as_ref()
                .rsplit('/')
                .next()
                .unwrap_or("checkpoint.parquet");
            let backup_path = OsPath::from(format!("{}/{}", backup_prefix.as_ref(), name));
            object_store
                .copy(path, &backup_path)
                .await
                .map_err(|error| {
                    Status::internal(format!(
                        "backup checkpoint {} to {} failed: {error}",
                        path.as_ref(),
                        backup_path.as_ref()
                    ))
                })?;
        }

        let mut sources = Vec::with_capacity(source_paths.len());
        for (path, meta) in source_paths.iter().zip(source_metas.into_iter()) {
            let bytes = read_object_chunked(&object_store, path, meta.size).await?;
            sources.push(CheckpointSource {
                path: path.clone(),
                meta,
                bytes,
            });
        }

        let (parts, rows) = write_checkpoint_parts(&sources, checkpoint.version, target_parts)?;
        if rows != checkpoint.size {
            return Err(Status::internal(format!(
                "checkpoint row mismatch: source _last_checkpoint says {}, parquet contains {}",
                checkpoint.size, rows
            )));
        }

        let staging_prefix = OsPath::from(format!(
            "_checkpoint_rewrite_staging/{}-v{}-parts{}",
            Utc::now().format("%Y%m%dT%H%M%SZ"),
            checkpoint.version,
            target_parts
        ));
        let mut staging_paths = Vec::with_capacity(parts.len());
        for part in &parts {
            let name = part
                .path
                .as_ref()
                .rsplit('/')
                .next()
                .unwrap_or("checkpoint.part.parquet");
            let staging_path = OsPath::from(format!("{}/{}", staging_prefix.as_ref(), name));
            object_store
                .put(&staging_path, part.bytes.clone().into())
                .await
                .map_err(|error| {
                    Status::internal(format!(
                        "write staging {} failed: {error}",
                        staging_path.as_ref()
                    ))
                })?;
            let meta = object_store.head(&staging_path).await.map_err(|error| {
                Status::internal(format!(
                    "HEAD staging {} failed: {error}",
                    staging_path.as_ref()
                ))
            })?;
            if meta.size != part.bytes.len() {
                return Err(Status::internal(format!(
                    "staging {} size mismatch: wrote {}, head returned {}",
                    staging_path.as_ref(),
                    part.bytes.len(),
                    meta.size
                )));
            }
            staging_paths.push(staging_path);
        }

        for (part, staging_path) in parts.iter().zip(staging_paths.iter()) {
            object_store
                .copy(staging_path, &part.path)
                .await
                .map_err(|error| {
                    Status::internal(format!(
                        "copy staging {} to final {} failed: {error}",
                        staging_path.as_ref(),
                        part.path.as_ref()
                    ))
                })?;
            let meta = object_store.head(&part.path).await.map_err(|error| {
                Status::internal(format!("HEAD final {} failed: {error}", part.path.as_ref()))
            })?;
            if meta.size != part.bytes.len() {
                return Err(Status::internal(format!(
                    "final {} size mismatch: expected {}, head returned {}",
                    part.path.as_ref(),
                    part.bytes.len(),
                    meta.size
                )));
            }
        }

        for source in &sources {
            object_store.delete(&source.path).await.map_err(|error| {
                Status::internal(format!(
                    "delete old checkpoint {} failed: {error}",
                    source.path.as_ref()
                ))
            })?;
        }

        let total_size: usize = parts.iter().map(|part| part.bytes.len()).sum();
        let max_part_size = parts.iter().map(|part| part.bytes.len()).max().unwrap_or(0);
        checkpoint.parts = Some(target_parts as i32);
        checkpoint.size_in_bytes = Some(total_size as i64);
        checkpoint.checksum = None;
        let new_last_checkpoint = Bytes::from(
            serde_json::to_vec(&checkpoint)
                .map_err(|error| Status::internal(format!("serialize checkpoint: {error}")))?,
        );
        object_store
            .put(&last_checkpoint_path, new_last_checkpoint.into())
            .await
            .map_err(|error| {
                Status::internal(format!("update _last_checkpoint failed: {error}"))
            })?;

        let manifest = serde_json::json!({
            "version": checkpoint.version,
            "source_parts": source_parts,
            "target_parts": target_parts,
            "source_size_bytes": source_size,
            "target_size_bytes": total_size,
            "max_part_size_bytes": max_part_size,
            "rows": rows,
            "source_files": sources.iter().map(|source| serde_json::json!({
                "path": source.path.as_ref(),
                "size": source.meta.size,
                "last_modified": source.meta.last_modified.to_rfc3339(),
            })).collect::<Vec<_>>(),
            "final_files": parts.iter().map(|part| serde_json::json!({
                "path": part.path.as_ref(),
                "size": part.bytes.len(),
                "rows": part.rows,
            })).collect::<Vec<_>>(),
        });
        object_store
            .put(
                &backup_prefix.child("rewrite-manifest.json"),
                Bytes::from(serde_json::to_vec_pretty(&manifest).map_err(|error| {
                    Status::internal(format!("serialize rewrite manifest failed: {error}"))
                })?)
                .into(),
            )
            .await
            .map_err(|error| Status::internal(format!("write rewrite manifest failed: {error}")))?;

        self.evict_table(&req.table_uri).await;

        Ok(Response::new(RewriteCheckpointMultipartResponse {
            version: checkpoint.version,
            source_parts: source_parts as i32,
            target_parts: target_parts as i32,
            source_size_bytes: source_size as i64,
            target_size_bytes: total_size as i64,
            max_part_size_bytes: max_part_size as i64,
            rows,
            rewritten: true,
            backup_prefix: backup_prefix.as_ref().to_string(),
            checkpoint_files: parts
                .iter()
                .map(|part| part.path.as_ref().to_string())
                .collect(),
            message: "checkpoint rewritten as multipart".into(),
        }))
    }

    // ── Vacuum ────────────────────────────────────────────────────────────────
    async fn vacuum(
        &self,
        request: Request<VacuumRequest>,
    ) -> Result<Response<VacuumResponse>, Status> {
        let req = request.into_inner();
        info!("vacuum uri={} dry_run={}", req.table_uri, req.dry_run);

        let table = self.get_or_open_table(&req.table_uri).await?;

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

        // Evict after vacuum since file list changed
        self.evict_table(&req.table_uri).await;

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
        use deltalake::storage::object_store::path::Path as OsPath;
        use deltalake::PartitionFilter;

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

        let mut table = self.get_or_open_table(&req.table_uri).await?;

        // ── Repair DuckDB size=1 bug ─────────────────────────────────────────
        {
            let log_store = table.log_store().clone();
            let snapshot = table.snapshot().map_err(internal)?.clone();

            // Current active files
            let mut all_adds: Vec<_> = snapshot
                .file_actions()
                .map_err(internal)?
                .into_iter()
                .collect();

            // Recovery: if there's a recent FSCK commit that incorrectly removed
            // size=1 files via Remove+Add, load the version just before that FSCK
            // and recover any dropped size=1 files.
            if table.version() > 0 {
                let history = table.history(Some(100)).await.unwrap_or_default();
                let mut seen_paths: std::collections::HashSet<String> =
                    all_adds.iter().map(|a| a.path.clone()).collect();
                let current_count = all_adds.len();
                for (idx, commit) in history.iter().enumerate() {
                    let fsck_version = table.version() - idx as i64;
                    if commit.operation.as_deref() == Some("FSCK") && fsck_version > 0 {
                        let pre_fsck_version = fsck_version - 1;
                        if let Ok(pre_fsck) = table_builder(&req.table_uri)
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
                        break;
                    }
                }
            }

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
                    let meta = object_store
                        .head(&path)
                        .await
                        .map_err(|e| Status::internal(format!("HEAD {} failed: {e}", add.path)))?;
                    let real_size = meta.size as i64;
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
        use deltalake::operations::optimize::OptimizeType;

        let mut builder = DeltaOps(table).optimize().with_target_size(target_size);
        if !partition_filters.is_empty() {
            builder = builder.with_filters(&partition_filters);
        }
        if !req.z_order_columns.is_empty() {
            builder = builder.with_type(OptimizeType::ZOrder(req.z_order_columns.clone()));
        }

        let (_, metrics) = builder.await.map_err(internal)?;

        // Evict after optimize since file list changed significantly
        self.evict_table(&req.table_uri).await;

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
