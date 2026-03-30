package deltago

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"time"

	deltapb "github.com/ghazibendahmane/go-delta-rs/gen/go/delta"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const grpcMaxMsgSize = 256 * 1024 * 1024 // 256 MiB

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

	// Env passes additional arbitrary environment variables to the sidecar.
	// Use this for provider-specific variables not covered by StorageConfig
	// (e.g. GCS or Azure credentials).
	Env []string

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
	cmd.Env = append(cmd.Env, s.opts.Env...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
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

// storageEnv translates StorageConfig fields into the environment variables
// consumed by delta-rs / object_store.
func storageEnv(cfg StorageConfig) []string {
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
