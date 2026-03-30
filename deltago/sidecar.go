package deltago

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"time"

	deltapb "github.com/ghazibendahmane/go-delta-rs/gen/go/delta"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// StorageConfig holds credentials and endpoint configuration for cloud storage.
// All fields are optional — unset fields fall back to the standard environment
// variables for each provider (AWS credential chain, GOOGLE_APPLICATION_CREDENTIALS, etc.).
type StorageConfig struct {
	// --- S3 / S3-compatible (MinIO, Localstack, Ceph, Tigris, …) ---

	// S3Endpoint overrides the default AWS endpoint URL.
	// Set this to use any S3-compatible storage:
	//   "http://localhost:9000"       → MinIO (local)
	//   "http://localhost:4566"       → Localstack
	//   "https://fly.storage.tigris.dev" → Tigris
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

	// S3ForcePathStyle forces path-style S3 URLs (s3.endpoint/bucket/key)
	// instead of virtual-hosted-style (bucket.s3.endpoint/key).
	// Required for MinIO and most self-hosted S3-compatible stores.
	S3ForcePathStyle bool
}

// SidecarOptions configures how the sidecar process is launched.
type SidecarOptions struct {
	// BinaryPath is the path to the compiled delta-server binary.
	// Defaults to "./delta-server/target/release/delta-server".
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
	// Defaults to 10 seconds.
	StartTimeout time.Duration
}

// Sidecar manages the lifetime of the delta-server subprocess.
type Sidecar struct {
	opts   SidecarOptions
	port   int
	cmd    *exec.Cmd
	conn   *grpc.ClientConn
	client deltapb.DeltaServiceClient
}

// NewSidecar creates a Sidecar but does not start it yet. Call Start.
func NewSidecar(opts SidecarOptions) *Sidecar {
	if opts.BinaryPath == "" {
		opts.BinaryPath = "./delta-server/target/release/delta-server"
	}
	if opts.StartTimeout == 0 {
		opts.StartTimeout = 10 * time.Second
	}
	return &Sidecar{opts: opts}
}

// Start launches the sidecar process and waits until it is healthy.
func (s *Sidecar) Start(ctx context.Context) error {
	port := s.opts.Port
	if port == 0 {
		var err error
		port, err = freePort()
		if err != nil {
			return fmt.Errorf("find free port: %w", err)
		}
	}
	s.port = port

	cmd := exec.CommandContext(ctx, s.opts.BinaryPath)
	cmd.Env = append(os.Environ(), fmt.Sprintf("DELTA_SERVER_PORT=%d", port))
	cmd.Env = append(cmd.Env, storageEnv(s.opts.Storage)...)
	cmd.Env = append(cmd.Env, s.opts.Env...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start delta-server: %w", err)
	}
	s.cmd = cmd

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		_ = cmd.Process.Kill()
		return fmt.Errorf("grpc dial: %w", err)
	}
	s.conn = conn
	s.client = deltapb.NewDeltaServiceClient(conn)

	if err := s.waitHealthy(ctx); err != nil {
		_ = s.Stop()
		return err
	}
	return nil
}

// Stop shuts down the sidecar process and closes the gRPC connection.
func (s *Sidecar) Stop() error {
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
	if cfg.S3ForcePathStyle {
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
