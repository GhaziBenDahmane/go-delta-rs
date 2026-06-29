package deltago

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"

	deltapb "github.com/ghazibendahmane/go-delta-rs/gen/go/delta"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const grpcMaxMsgSize = 256 * 1024 * 1024 // 256 MiB

// S3CommitMode selects the generic object-store mechanism used for Delta log
// commits on S3-compatible storage.
type S3CommitMode string

const (
	// S3CommitModeUnsafeRename uses delta-rs' unsafe rename fallback.
	S3CommitModeUnsafeRename S3CommitMode = "unsafe_rename"
	// S3CommitModeConditionalPutETag uses object_store conditional put with ETag preconditions.
	S3CommitModeConditionalPutETag S3CommitMode = "conditional_put:etag"
	// S3CommitModeCopyIfNotExistsMultipart uses object_store multipart copy-if-not-exists.
	S3CommitModeCopyIfNotExistsMultipart S3CommitMode = "copy_if_not_exists:multipart"
	// S3CommitModeDynamoPrefix is the prefix for DynamoDB-backed coordination,
	// e.g. "dynamo:delta-locks" or "dynamo:delta-locks:5000".
	S3CommitModeDynamoPrefix S3CommitMode = "dynamo:"
)

// RuntimeProfile selects a preset for sidecar memory/runtime behavior.
type RuntimeProfile string

const (
	// RuntimeProfileBalanced keeps the sidecar defaults.
	RuntimeProfileBalanced RuntimeProfile = "balanced"
	// RuntimeProfileLowRSS bounds the table cache and enables allocator purging.
	RuntimeProfileLowRSS RuntimeProfile = "low_rss"
	// RuntimeProfileMinimumMemory disables the table cache and purges more aggressively.
	RuntimeProfileMinimumMemory RuntimeProfile = "minimum_memory"
)

// StorageConfig holds credentials and endpoint configuration for cloud storage.
// All fields are optional — unset fields fall back to the standard environment
// variables for each provider (AWS credential chain, GOOGLE_APPLICATION_CREDENTIALS, etc.).
type StorageConfig struct {
	// --- S3 / S3-compatible (MinIO, Localstack, Ceph, Dell ECS, …) ---

	// S3Endpoint overrides the default AWS endpoint URL.
	// Set this to use any S3-compatible storage:
	//   "http://localhost:9000"              → MinIO (local)
	//   "http://localhost:4566"              → Localstack
	//   "https://vip-ecs.example.com"        → Dell ECS or Ceph
	S3Endpoint string

	// S3AllowHTTP allows plain HTTP connections. Required for local MinIO or
	// Localstack when TLS is not configured.
	S3AllowHTTP bool

	// S3AccessKeyID overrides AWS_ACCESS_KEY_ID.
	S3AccessKeyID string

	// S3SecretAccessKey overrides AWS_SECRET_ACCESS_KEY.
	S3SecretAccessKey string

	// S3Region overrides AWS_DEFAULT_REGION.
	S3Region string

	// S3AllowUnsafeRename disables atomic rename checks required by delta-rs
	// when writing to S3-compatible stores that do not support atomic renames
	// (e.g. Dell ECS, MinIO, Ceph). Set this to true for any non-AWS S3 target.
	S3AllowUnsafeRename bool

	// S3CommitMode is the preferred high-level way to configure Delta log commit
	// behavior. It replaces setting S3AllowUnsafeRename, S3ConditionalPut, and
	// S3CopyIfNotExists directly for common modes.
	S3CommitMode S3CommitMode

	// S3ConditionalPut configures object_store conditional put support.
	// Supported values depend on object_store. Common values are "etag" and
	// "dynamo:<TABLE_NAME>[:TIMEOUT_MILLIS]".
	S3ConditionalPut string

	// S3CopyIfNotExists configures object_store copy-if-not-exists support.
	// Common values are "multipart", "header:<KEY>:<VALUE>",
	// "header-with-status:<KEY>:<VALUE>:<STATUS>", and
	// "dynamo:<TABLE_NAME>[:TIMEOUT_MILLIS]".
	S3CopyIfNotExists string

	// S3ChecksumAlgorithm configures upload checksums. object_store 0.11
	// supports "sha256".
	S3ChecksumAlgorithm string

	// S3ForcePathStyle forces path-style S3 URLs (e.g. https://host/bucket/key)
	// instead of virtual-hosted-style (e.g. https://bucket.host/key).
	// Required for Dell ECS, MinIO, Ceph, and most self-hosted S3-compatible stores.
	S3ForcePathStyle bool
}

// RuntimeConfig controls generic sidecar runtime memory behavior.
type RuntimeConfig struct {
	// Profile applies a memory/runtime preset. Explicit fields below override
	// the selected profile.
	Profile RuntimeProfile

	// DisableTableCache forces the sidecar to load fresh Delta table state for
	// each operation instead of retaining DeltaTable snapshots in memory.
	DisableTableCache bool

	// TableCacheMaxEntries bounds cached DeltaTable snapshots. 0 uses the
	// sidecar default. Ignored when DisableTableCache is true.
	TableCacheMaxEntries int

	// TableCacheTTL evicts cached DeltaTable snapshots after this idle time.
	// 0 disables TTL eviction.
	TableCacheTTL time.Duration

	// MemoryPurgeInterval periodically asks jemalloc to return unused pages to
	// the operating system. 0 disables periodic purge.
	MemoryPurgeInterval time.Duration

	// JemallocBackgroundThread enables jemalloc background purging for sidecars
	// launched by this Go process.
	JemallocBackgroundThread bool

	// JemallocDecay controls dirty/muzzy page decay in MALLOC_CONF. 0 leaves the
	// jemalloc default unchanged.
	JemallocDecay time.Duration
}

// Validate returns an error for incompatible storage settings.
func (c StorageConfig) Validate() error {
	resolved, err := resolveStorageConfig(c)
	if err != nil {
		return err
	}
	c = resolved
	if c.S3AllowUnsafeRename && (c.S3ConditionalPut != "" || c.S3CopyIfNotExists != "") {
		return fmt.Errorf("S3AllowUnsafeRename cannot be combined with conditional S3 commit settings")
	}
	if c.S3ConditionalPut != "" {
		v := strings.TrimSpace(strings.ToLower(c.S3ConditionalPut))
		if v != "etag" && !strings.HasPrefix(v, "dynamo:") {
			return fmt.Errorf("unsupported S3ConditionalPut %q", c.S3ConditionalPut)
		}
	}
	if c.S3CopyIfNotExists != "" {
		v := strings.TrimSpace(strings.ToLower(c.S3CopyIfNotExists))
		if v != "multipart" &&
			!strings.HasPrefix(v, "header:") &&
			!strings.HasPrefix(v, "header-with-status:") &&
			!strings.HasPrefix(v, "dynamo:") {
			return fmt.Errorf("unsupported S3CopyIfNotExists %q", c.S3CopyIfNotExists)
		}
	}
	if c.S3ChecksumAlgorithm != "" && !strings.EqualFold(strings.TrimSpace(c.S3ChecksumAlgorithm), "sha256") {
		return fmt.Errorf("unsupported S3ChecksumAlgorithm %q", c.S3ChecksumAlgorithm)
	}
	return nil
}

func resolveStorageConfig(cfg StorageConfig) (StorageConfig, error) {
	rawMode := strings.TrimSpace(string(cfg.S3CommitMode))
	mode := strings.ToLower(rawMode)
	if mode == "" {
		return cfg, nil
	}
	if cfg.S3AllowUnsafeRename || cfg.S3ConditionalPut != "" || cfg.S3CopyIfNotExists != "" {
		return cfg, fmt.Errorf("S3CommitMode cannot be combined with S3AllowUnsafeRename, S3ConditionalPut, or S3CopyIfNotExists")
	}

	switch {
	case mode == string(S3CommitModeUnsafeRename):
		cfg.S3AllowUnsafeRename = true
	case mode == "etag" || mode == string(S3CommitModeConditionalPutETag):
		cfg.S3ConditionalPut = "etag"
	case mode == "multipart" || mode == string(S3CommitModeCopyIfNotExistsMultipart):
		cfg.S3CopyIfNotExists = "multipart"
	case strings.HasPrefix(mode, string(S3CommitModeDynamoPrefix)):
		cfg.S3ConditionalPut = rawMode
		cfg.S3CopyIfNotExists = rawMode
	case strings.HasPrefix(mode, "conditional_put:"):
		cfg.S3ConditionalPut = rawMode[len("conditional_put:"):]
	case strings.HasPrefix(mode, "copy_if_not_exists:"):
		cfg.S3CopyIfNotExists = rawMode[len("copy_if_not_exists:"):]
	default:
		return cfg, fmt.Errorf("unsupported S3CommitMode %q", cfg.S3CommitMode)
	}
	return cfg, nil
}

func resolveRuntimeConfig(cfg RuntimeConfig) (RuntimeConfig, error) {
	profile := strings.TrimSpace(strings.ToLower(string(cfg.Profile)))
	switch RuntimeProfile(profile) {
	case "", RuntimeProfileBalanced:
		return cfg, nil
	case RuntimeProfileLowRSS:
		if cfg.TableCacheMaxEntries == 0 && !cfg.DisableTableCache {
			cfg.TableCacheMaxEntries = 2
		}
		if cfg.MemoryPurgeInterval == 0 {
			cfg.MemoryPurgeInterval = time.Minute
		}
		if cfg.JemallocDecay == 0 {
			cfg.JemallocDecay = 5 * time.Second
		}
		cfg.JemallocBackgroundThread = true
	case RuntimeProfileMinimumMemory:
		cfg.DisableTableCache = true
		if cfg.MemoryPurgeInterval == 0 {
			cfg.MemoryPurgeInterval = 30 * time.Second
		}
		if cfg.JemallocDecay == 0 {
			cfg.JemallocDecay = time.Second
		}
		cfg.JemallocBackgroundThread = true
	default:
		return cfg, fmt.Errorf("unsupported RuntimeProfile %q", cfg.Profile)
	}
	return cfg, nil
}

// SidecarOptions configures how the sidecar process is launched.
type SidecarOptions struct {
	// BinaryPath is the path to the delta-server binary.
	// Leave empty (the default) to have the binary downloaded automatically
	// from GitHub Releases and cached in os.UserCacheDir() on first use.
	BinaryPath string

	// Port for the gRPC server. 0 = pick a free port automatically.
	Port int

	// Storage holds cloud storage endpoint and credential overrides.
	Storage StorageConfig

	// Runtime controls generic sidecar cache and allocator behavior.
	Runtime RuntimeConfig

	// Env passes additional arbitrary environment variables to the sidecar.
	// Use this for provider-specific variables not covered by StorageConfig
	// (e.g. GCS or Azure credentials).
	Env []string

	// Stdout and Stderr receive sidecar process output. Nil uses os.Stdout and
	// os.Stderr respectively.
	Stdout io.Writer
	Stderr io.Writer

	// StartTimeout is how long to wait for the sidecar to become healthy.
	// Defaults to 30 seconds.
	StartTimeout time.Duration
}

// Sidecar manages the lifetime of the delta-server subprocess.
type Sidecar struct {
	opts       SidecarOptions
	port       int
	binaryPath string
	cmd        *exec.Cmd
	conn       *grpc.ClientConn
	client     deltapb.DeltaServiceClient
	stopCh     chan struct{}
}

// NewSidecar creates a Sidecar but does not start it yet. Call Start.
func NewSidecar(opts SidecarOptions) *Sidecar {
	if opts.StartTimeout == 0 {
		opts.StartTimeout = 30 * time.Second
	}
	return &Sidecar{
		opts:   opts,
		stopCh: make(chan struct{}),
	}
}

// Start launches the sidecar process and waits until it is healthy.
// If BinaryPath is empty, the binary is downloaded automatically from
// GitHub Releases and cached locally (requires internet access on first run).
// A monitor goroutine is started to automatically restart the process if it
// exits unexpectedly.
func (s *Sidecar) Start(ctx context.Context) error {
	if err := s.opts.Storage.Validate(); err != nil {
		return err
	}
	if _, err := resolveRuntimeConfig(s.opts.Runtime); err != nil {
		return err
	}

	binaryPath := s.opts.BinaryPath
	if binaryPath == "" {
		var err error
		binaryPath, err = EnsureBinary()
		if err != nil {
			return fmt.Errorf("ensure delta-server binary: %w", err)
		}
	}
	s.binaryPath = binaryPath

	port := s.opts.Port
	if port == 0 {
		var err error
		port, err = freePort()
		if err != nil {
			return fmt.Errorf("find free port: %w", err)
		}
	}
	s.port = port

	if err := s.startProcess(); err != nil {
		return err
	}

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(grpcMaxMsgSize),
			grpc.MaxCallSendMsgSize(grpcMaxMsgSize),
		),
	)
	if err != nil {
		_ = s.cmd.Process.Kill()
		return fmt.Errorf("grpc dial: %w", err)
	}
	s.conn = conn
	s.client = deltapb.NewDeltaServiceClient(conn)

	if err := s.waitHealthy(ctx); err != nil {
		_ = s.Stop()
		return err
	}

	go s.monitor(ctx)
	return nil
}

// startProcess launches the delta-server binary. The gRPC connection is reused
// across restarts — gRPC reconnects automatically when the server comes back up
// on the same port.
func (s *Sidecar) startProcess() error {
	cmd := exec.Command(s.binaryPath)
	cmd.Env = append(os.Environ(), fmt.Sprintf("DELTA_SERVER_PORT=%d", s.port))
	cmd.Env = append(cmd.Env, storageEnv(s.opts.Storage)...)
	cmd.Env = append(cmd.Env, runtimeEnv(s.opts.Runtime)...)
	cmd.Env = append(cmd.Env, s.opts.Env...)
	cmd.Stdout = s.opts.Stdout
	if cmd.Stdout == nil {
		cmd.Stdout = os.Stdout
	}
	cmd.Stderr = s.opts.Stderr
	if cmd.Stderr == nil {
		cmd.Stderr = os.Stderr
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start delta-server: %w", err)
	}
	s.cmd = cmd
	return nil
}

// monitor waits for the process to exit and restarts it unless Stop has been
// called. The gRPC client connection is preserved across restarts.
func (s *Sidecar) monitor(ctx context.Context) {
	for {
		err := s.cmd.Wait()

		// Check both the stop channel and the context before restarting.
		select {
		case <-s.stopCh:
			return
		default:
		}
		if ctx.Err() != nil {
			return
		}

		slog.Error("delta-server exited unexpectedly, restarting",
			"error", err,
			"port", s.port,
		)

		select {
		case <-s.stopCh:
			return
		case <-time.After(500 * time.Millisecond):
		}

		if err := s.startProcess(); err != nil {
			slog.Error("delta-server restart failed", "error", err)
			return
		}

		if err := s.waitHealthy(ctx); err != nil {
			slog.Error("delta-server health check failed after restart", "error", err)
			return
		}

		slog.Info("delta-server restarted successfully", "port", s.port)
	}
}

// Stop shuts down the sidecar process and closes the gRPC connection.
// Safe to call multiple times.
func (s *Sidecar) Stop() error {
	select {
	case <-s.stopCh:
		// already stopped
	default:
		close(s.stopCh)
	}
	if s.conn != nil {
		_ = s.conn.Close()
	}
	if s.cmd != nil && s.cmd.Process != nil {
		return s.cmd.Process.Kill()
	}
	return nil
}

// Client returns a DeltaClient backed by this sidecar.
func (s *Sidecar) Client() *DeltaClient {
	return &DeltaClient{client: s.client}
}

// Port returns the port the sidecar is listening on.
func (s *Sidecar) Port() int { return s.port }

func (s *Sidecar) waitHealthy(ctx context.Context) error {
	deadline := time.Now().Add(s.opts.StartTimeout)
	for time.Now().Before(deadline) {
		hctx, cancel := context.WithTimeout(ctx, time.Second)
		_, err := s.client.Health(hctx, &deltapb.HealthRequest{})
		cancel()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return fmt.Errorf("delta-server did not become healthy within %s", s.opts.StartTimeout)
}

// runtimeEnv translates RuntimeConfig fields into sidecar environment
// variables. Values in SidecarOptions.Env are appended after these and can
// override them when needed.
func runtimeEnv(cfg RuntimeConfig) []string {
	cfg, _ = resolveRuntimeConfig(cfg)
	var env []string
	if cfg.DisableTableCache {
		env = append(env, "DELTA_TABLE_CACHE_MAX_ENTRIES=0")
	} else if cfg.TableCacheMaxEntries > 0 {
		env = append(env, fmt.Sprintf("DELTA_TABLE_CACHE_MAX_ENTRIES=%d", cfg.TableCacheMaxEntries))
	}
	if cfg.TableCacheTTL > 0 {
		env = append(env, fmt.Sprintf("DELTA_TABLE_CACHE_TTL_SECONDS=%d", int64(cfg.TableCacheTTL.Seconds())))
	}
	if cfg.MemoryPurgeInterval > 0 {
		env = append(env, fmt.Sprintf("DELTA_MEMORY_PURGE_INTERVAL_SECONDS=%d", int64(cfg.MemoryPurgeInterval.Seconds())))
	}

	var mallocConf []string
	if cfg.JemallocBackgroundThread {
		mallocConf = append(mallocConf, "background_thread:true")
	}
	if cfg.JemallocDecay > 0 {
		decayMS := cfg.JemallocDecay.Milliseconds()
		mallocConf = append(mallocConf,
			fmt.Sprintf("dirty_decay_ms:%d", decayMS),
			fmt.Sprintf("muzzy_decay_ms:%d", decayMS),
		)
	}
	if len(mallocConf) > 0 {
		env = append(env, "MALLOC_CONF="+strings.Join(mallocConf, ","))
	}
	return env
}

// storageEnv translates StorageConfig fields into the environment variables
// consumed by delta-rs / object_store.
func storageEnv(cfg StorageConfig) []string {
	cfg, _ = resolveStorageConfig(cfg)
	var env []string
	set := func(k, v string) {
		if v != "" {
			env = append(env, k+"="+v)
		}
	}
	set("AWS_ENDPOINT_URL", cfg.S3Endpoint)
	set("AWS_ACCESS_KEY_ID", cfg.S3AccessKeyID)
	set("AWS_SECRET_ACCESS_KEY", cfg.S3SecretAccessKey)
	set("AWS_DEFAULT_REGION", cfg.S3Region)
	if cfg.S3AllowHTTP {
		env = append(env, "AWS_ALLOW_HTTP=true")
	}
	if cfg.S3AllowUnsafeRename {
		env = append(env, "AWS_S3_ALLOW_UNSAFE_RENAME=true")
	}
	set("AWS_CONDITIONAL_PUT", cfg.S3ConditionalPut)
	set("AWS_S3_CONDITIONAL_PUT", cfg.S3ConditionalPut)
	set("AWS_COPY_IF_NOT_EXISTS", cfg.S3CopyIfNotExists)
	set("AWS_S3_COPY_IF_NOT_EXISTS", cfg.S3CopyIfNotExists)
	set("AWS_CHECKSUM_ALGORITHM", cfg.S3ChecksumAlgorithm)
	if cfg.S3ForcePathStyle {
		env = append(env, "AWS_S3_FORCE_PATH_STYLE=true")
	}
	return env
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port, nil
}
