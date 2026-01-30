package database

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/myorg/pg_tuner/pg_workload/internal/config"
)

// Pool wraps pgxpool.Pool with configuration and helper methods.
type Pool struct {
	pool *pgxpool.Pool
	cfg  *config.DatabaseConfig
}

// PoolConfig holds pool-specific settings.
type PoolConfig struct {
	MinConns          int32
	MaxConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
}

// DefaultPoolConfig returns sensible default pool settings.
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MinConns:          2,
		MaxConns:          10,
		MaxConnLifetime:   30 * time.Minute,
		MaxConnIdleTime:   5 * time.Minute,
		HealthCheckPeriod: 30 * time.Second,
	}
}

// NewPool creates a new database connection pool.
func NewPool(ctx context.Context, cfg *config.DatabaseConfig) (*Pool, error) {
	return NewPoolWithConfig(ctx, cfg, DefaultPoolConfig())
}

// NewPoolWithConfig creates a new database connection pool with custom pool settings.
func NewPoolWithConfig(ctx context.Context, cfg *config.DatabaseConfig, poolCfg PoolConfig) (*Pool, error) {
	connStr := cfg.ConnectionString()

	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("parsing connection string: %w", err)
	}

	poolConfig.MinConns = poolCfg.MinConns
	poolConfig.MaxConns = poolCfg.MaxConns
	poolConfig.MaxConnLifetime = poolCfg.MaxConnLifetime
	poolConfig.MaxConnIdleTime = poolCfg.MaxConnIdleTime
	poolConfig.HealthCheckPeriod = poolCfg.HealthCheckPeriod

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("creating connection pool: %w", err)
	}

	return &Pool{
		pool: pool,
		cfg:  cfg,
	}, nil
}

// Close closes all connections in the pool.
func (p *Pool) Close() {
	if p.pool != nil {
		p.pool.Close()
	}
}

// HealthCheck verifies the database connection is healthy.
func (p *Pool) HealthCheck(ctx context.Context) error {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Release()

	if err := conn.Conn().Ping(ctx); err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}

	return nil
}

// Acquire gets a connection from the pool.
func (p *Pool) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	return p.pool.Acquire(ctx)
}

// Exec executes a query without returning rows.
func (p *Pool) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := p.pool.Exec(ctx, sql, args...)
	return err
}

// Pool returns the underlying pgxpool.Pool.
func (p *Pool) Pool() *pgxpool.Pool {
	return p.pool
}

// Stats returns pool statistics.
func (p *Pool) Stats() *pgxpool.Stat {
	return p.pool.Stat()
}
