# pg_workload Quick Start Guide

## Requirements

- Go 1.21 or later
- PostgreSQL 12+ (local or remote)
- Make (optional, for convenience targets)

## Installation

```bash
# Clone and build
cd ~/develop/pg_tuner/pg_workload
make build

# Or install to GOPATH/bin
make install
```

## Database Setup

Ensure PostgreSQL is accessible. The tool uses standard PostgreSQL environment variables:

```bash
export PGHOST=localhost
export PGPORT=5432
export PGUSER=postgres
export PGPASSWORD=yourpassword
export PGDATABASE=postgres
```

Or use a config file (see `configs/example.yaml`).

## First Run

### Quick 1-minute test

```bash
./pg_workload run \
  --duration 1m \
  --warmup 10s \
  --cooldown 10s \
  --output report.json
```

### Standard 15-minute benchmark

```bash
./pg_workload run \
  --duration 15m \
  --warmup 2m \
  --cooldown 1m \
  --workers 4 \
  --connections 10 \
  --output benchmark.json
```

### Using Make

```bash
# Run smoke test (1 minute)
make smoke-test

# Run with custom PostgreSQL host
PGHOST=mydb.example.com make smoke-test
```

## Command Reference

```bash
# Show help
./pg_workload --help
./pg_workload run --help

# Show version
./pg_workload version
```

## Key Options

| Flag | Default | Description |
|------|---------|-------------|
| `--duration` | 15m | Test duration (minimum 1m) |
| `--warmup` | 2m | Warmup period before measuring |
| `--cooldown` | 1m | Cooldown period after test |
| `--workers` | 4 | Concurrent worker goroutines |
| `--connections` | 10 | Database connection pool size |
| `--seed` | 42 | Random seed for reproducibility |
| `--scale` | 1 | Data scale (1 = 10K accounts) |
| `--output` | stdout | Output file for JSON report |
| `--quiet` | false | Suppress progress output |
| `--skip-setup` | false | Skip schema creation |
| `--skip-cleanup` | false | Keep schema after run |

## Output

The tool produces a JSON report with:

- **run_info**: Execution metadata (duration, workers, seed)
- **summary**: Aggregate metrics (total queries, QPS, error rate)
- **latencies**: Per-operation breakdown with percentiles (p50, p90, p95, p99)
- **errors**: Error classification by type

Example output:

```json
{
  "version": "1.0",
  "summary": {
    "total_queries": 125000,
    "qps": 2083.33,
    "error_rate_pct": 0.01
  },
  "latencies": {
    "point_select": {
      "count": 50000,
      "p50": "1.2ms",
      "p99": "5.8ms"
    }
  }
}
```

## Workload Profile

The default `oltp_standard` profile includes:

| Query | Type | Weight |
|-------|------|--------|
| point_select | read | 40% |
| range_select | read | 20% |
| insert_tx | write | 20% |
| update_balance | write | 15% |
| complex_join | read | 5% |

Total: 65% reads, 35% writes

## Troubleshooting

### Connection refused

```bash
# Check PostgreSQL is running
pg_isready -h localhost -p 5432

# Verify credentials
psql -h localhost -U postgres -c "SELECT 1"
```

### Permission denied

Ensure the PostgreSQL user has CREATE TABLE privileges:

```sql
GRANT ALL ON DATABASE postgres TO postgres;
```

### Low QPS

- Increase `--workers` (try 8-16)
- Increase `--connections` (try 20-50)
- Check PostgreSQL server resources
- Verify network latency if remote

## Next Steps

- Review full report JSON structure
- Experiment with different scale factors
- Compare results across configurations
- Integrate with pg_tuner for automated tuning
