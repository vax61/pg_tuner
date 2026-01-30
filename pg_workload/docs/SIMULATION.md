# Simulation Mode

Simulation mode provides time-compressed workload simulation, allowing you to simulate hours or days of database activity in a fraction of the time. This is useful for:

- Testing database behavior under realistic time-varying load patterns
- Validating autoscaling and resource management configurations
- Analyzing long-term performance trends without waiting for actual time to pass
- Studying the impact of scheduled events (maintenance windows, batch jobs, etc.)

## Quick Start

```bash
# Run a 24-hour simulation compressed to 2 hours (12x time scale)
pg_workload run \
  --mode simulation \
  --duration 24h \
  --time-scale 12 \
  --timeline-output timeline.csv \
  --output report.json
```

## Command Line Options

### Simulation-Specific Options

| Flag | Default | Description |
|------|---------|-------------|
| `--mode` | `burst` | Set to `simulation` for simulation mode |
| `--time-scale` | `1` | Time compression factor (1-24). A value of 12 means 1 real hour = 12 simulated hours |
| `--start-time` | now | Simulation start time: `HH:MM` or `YYYY-MM-DDTHH:MM:SS` |
| `--clock` | `simulated` | Clock mode: `real` or `simulated` |
| `--max-storage` | `500MB` | Maximum storage for simulation data |
| `--raw-retention` | `10m` | Rolling window for raw metrics data |
| `--aggregate-interval` | `1m` | Aggregation granularity for timeline output |
| `--timeline-output` | - | CSV file path for timeline data |

### Common Options

These options work in both burst and simulation modes:

| Flag | Default | Description |
|------|---------|-------------|
| `--duration` | `15m` | Simulated duration (simulation mode) |
| `--workers` | `4` | Number of worker goroutines |
| `--connections` | `10` | Number of database connections |
| `--seed` | `42` | Random seed for reproducibility |
| `--profile` | `oltp_standard` | Workload profile name |
| `--output` | stdout | JSON report output file |

## Time Compression

The `--time-scale` flag controls how fast simulated time passes relative to real time:

| Time Scale | Real Time | Simulated Time |
|------------|-----------|----------------|
| 1 | 1 hour | 1 hour |
| 6 | 1 hour | 6 hours |
| 12 | 1 hour | 12 hours |
| 24 | 1 hour | 24 hours |

**Example**: Simulating a full day in 1 hour:

```bash
pg_workload run \
  --mode simulation \
  --duration 24h \
  --time-scale 24 \
  --start-time 00:00
```

## Load Patterns

Simulation mode uses load patterns defined in YAML profile files. The profile controls how load varies over time.

### Built-in Patterns

**Business Hours Pattern** (`oltp_business_hours`):
- Higher load during business hours (9 AM - 6 PM)
- Lower load during evening and night
- Configurable peak and off-peak multipliers

**Constant Pattern**:
- Steady load throughout the simulation
- Good for baseline comparisons

### Example Profile

```yaml
name: custom_pattern
description: Custom time-varying load pattern

workload_distribution:
  read: 70
  write: 30

load_pattern:
  type: daily
  baseline_qps: 100
  time_segments:
    - start: "00:00"
      end: "06:00"
      multiplier: 0.2    # Night: 20% of baseline
    - start: "06:00"
      end: "09:00"
      multiplier: 0.8    # Morning ramp-up
    - start: "09:00"
      end: "12:00"
      multiplier: 1.5    # Morning peak
    - start: "12:00"
      end: "13:00"
      multiplier: 1.0    # Lunch dip
    - start: "13:00"
      end: "17:00"
      multiplier: 1.5    # Afternoon peak
    - start: "17:00"
      end: "21:00"
      multiplier: 0.8    # Evening wind-down
    - start: "21:00"
      end: "24:00"
      multiplier: 0.3    # Night
```

## Scheduled Events

Events can be scheduled to modify workload behavior at specific times:

```yaml
events:
  - name: maintenance_window
    schedule: "0 2 * * *"     # 2 AM daily (cron format)
    duration: 30m
    effect:
      multiplier: 0.1        # Reduce load to 10%

  - name: batch_job
    schedule: "0 3 * * 1"    # 3 AM every Monday
    duration: 1h
    effect:
      multiplier: 2.0        # Double the load
      read_ratio: 20         # More writes during batch
```

## Timeline Output

The `--timeline-output` flag produces a CSV file with metrics over time:

```csv
timestamp,queries,errors,qps,avg_latency_us,p50_latency_us,p95_latency_us,p99_latency_us,workers,read_queries,write_queries
2025-01-01T00:00:00Z,5432,12,90.5,1234,1100,2500,5000,4,3802,1630
2025-01-01T00:01:00Z,6789,8,113.2,1156,1050,2300,4800,5,4752,2037
...
```

Use this data to:
- Visualize load patterns over time
- Identify performance trends
- Correlate latency with QPS
- Analyze the impact of scheduled events

## Report Output

Simulation mode produces an extended JSON report:

```json
{
  "version": "1.0",
  "run_info": {
    "start_time": "2025-01-01T10:00:00Z",
    "end_time": "2025-01-01T12:00:00Z",
    "duration": "2h0m0s",
    "mode": "simulation",
    "profile": "oltp_business_hours"
  },
  "simulation_info": {
    "time_scale": 12,
    "start_sim_time": "2025-01-01T00:00:00Z",
    "end_sim_time": "2025-01-02T00:00:00Z",
    "simulated_duration": "24h0m0s",
    "real_duration": "2h0m0s",
    "profile_used": "oltp_business_hours",
    "clock_mode": "simulated"
  },
  "summary": {
    "total_queries": 1234567,
    "total_errors": 123,
    "qps": 171.3,
    "error_rate": 0.01,
    "read_queries": 864197,
    "write_queries": 370370
  },
  "timeline_summary": {
    "intervals": 1440,
    "avg_qps": 171.3,
    "min_qps": 20.5,
    "max_qps": 250.8,
    "avg_latency_us": 1234,
    "p95_latency_us": 2500
  },
  "events_triggered": [
    {
      "name": "maintenance_window",
      "start_time": "2025-01-01T02:00:00Z",
      "end_time": "2025-01-01T02:30:00Z",
      "triggered": true
    }
  ],
  "storage_used_bytes": 52428800
}
```

## Storage Management

Simulation mode manages storage automatically to prevent unbounded growth:

- **Raw data retention**: Controlled by `--raw-retention`, raw metrics are kept in a rolling window
- **Aggregated data**: Stored at `--aggregate-interval` granularity for the full simulation
- **Maximum storage**: Controlled by `--max-storage`, simulation stops if limit is reached

Storage location: `/tmp/pg_workload_sim` (configurable)

## Best Practices

1. **Start with short simulations**: Verify behavior with a 1-hour simulation before running longer ones

2. **Choose appropriate time scale**: Higher time scales reduce accuracy. For precise measurements, use lower time scales.

3. **Monitor storage**: For multi-day simulations, ensure adequate `--max-storage`

4. **Use consistent seeds**: The `--seed` flag ensures reproducible workload patterns

5. **Match connections to workers**: Generally use 2x connections vs workers for connection pool efficiency

## Examples

### Simulate a Full Day

```bash
pg_workload run \
  --mode simulation \
  --duration 24h \
  --time-scale 24 \
  --start-time 00:00 \
  --profile oltp_business_hours \
  --timeline-output day_timeline.csv \
  --output day_report.json
```

### Simulate Peak Hours Only

```bash
pg_workload run \
  --mode simulation \
  --duration 8h \
  --time-scale 8 \
  --start-time 09:00 \
  --workers 8 \
  --connections 16 \
  --output peak_report.json
```

### Long-Running Simulation (1 Week)

```bash
pg_workload run \
  --mode simulation \
  --duration 168h \
  --time-scale 24 \
  --max-storage 2GB \
  --aggregate-interval 5m \
  --raw-retention 30m \
  --timeline-output week_timeline.csv \
  --output week_report.json
```

## Troubleshooting

### Simulation ends early

Check the report for `storage_used_bytes`. If it's near `max-storage`, increase the limit:

```bash
--max-storage 1GB
```

### Low actual QPS compared to target

- Increase `--workers` and `--connections`
- Check database connection limits
- Verify database can handle the target QPS

### Timeline data is sparse

Decrease `--aggregate-interval` for more granular data:

```bash
--aggregate-interval 30s
```

### Memory usage grows unbounded

Decrease `--raw-retention` to keep less raw data in memory:

```bash
--raw-retention 5m
```
