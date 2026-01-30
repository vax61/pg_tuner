"""Pydantic schemas for pg_tuner MCP tools.

These schemas define the input/output contracts for MCP tools
as specified in api-contracts.md.
"""

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


# PostgreSQL Statistics Models


class BgwriterStats(BaseModel):
    """PostgreSQL background writer statistics."""

    checkpoints_timed: int = 0
    checkpoints_req: int = 0
    checkpoint_write_time: float = 0.0
    checkpoint_sync_time: float = 0.0
    buffers_checkpoint: int = 0
    buffers_clean: int = 0
    maxwritten_clean: int = 0
    buffers_backend: int = 0
    buffers_backend_fsync: int = 0
    buffers_alloc: int = 0


class StatementStats(BaseModel):
    """pg_stat_statements entry."""

    queryid: int
    query: str
    calls: int
    total_exec_time: float
    mean_exec_time: float
    rows: int
    shared_blks_hit: int
    shared_blks_read: int


class PgStatsResult(BaseModel):
    """Result from collect_pg_stats tool."""

    timestamp: datetime
    cache_hit_ratio: float = Field(ge=0.0, le=1.0)
    bgwriter: BgwriterStats
    statements: list[StatementStats] = Field(default_factory=list)


# OS Metrics Models


class IostatResult(BaseModel):
    """iostat metrics for a device."""

    device: str
    r_per_s: float = 0.0
    w_per_s: float = 0.0
    rkb_per_s: float = 0.0
    wkb_per_s: float = 0.0
    await_ms: float = 0.0
    util_pct: float = 0.0


class OsMetricsResult(BaseModel):
    """Result from collect_os_metrics tool."""

    timestamp: datetime
    iostat: Optional[IostatResult] = None
    memory_used_pct: float = Field(ge=0.0, le=100.0)
    cpu_iowait_pct: float = Field(ge=0.0, le=100.0)


# Hardware Info Models


class HwInfoResult(BaseModel):
    """Result from collect_hw_info tool."""

    cpu_cores_physical: int
    cpu_cores_logical: int
    ram_gb: float
    storage_type: str = Field(description="SSD, HDD, or NVMe")
    numa_nodes: int = 1


# Workload Models


class WorkloadConfig(BaseModel):
    """Configuration for pg_workload execution."""

    profile: str = "oltp"
    duration: str = "60s"
    workers: int = 4
    connections: int = 10


class WorkloadResult(BaseModel):
    """Result from run_workload tool (pg_workload JSON format)."""

    mode: str
    duration_seconds: float
    total_transactions: int
    tps: float
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    errors: int = 0


# Recommendation Models


class RecommendationResult(BaseModel):
    """A single tuning recommendation."""

    category: str = Field(description="memory, io, wal, connections, etc.")
    parameter: str
    current_value: str
    suggested_value: str
    confidence: str = Field(description="high, medium, low")
    restart_required: bool = False
    evidence: list[str] = Field(default_factory=list)
    impact: str = ""
    risk: str = ""


# Report Analysis Models


class BurstReportAnalysis(BaseModel):
    """Result from analyze_burst_report tool."""

    status: str = "success"
    report_path: str
    workload: Optional[WorkloadResult] = None
    recommendations: list[RecommendationResult] = Field(default_factory=list)
    summary: str = ""


class SimulationReportAnalysis(BaseModel):
    """Result from analyze_simulation_report tool."""

    status: str = "success"
    report_path: str
    timeline_path: Optional[str] = None
    workload: Optional[WorkloadResult] = None
    timeline_points: int = 0
    recommendations: list[RecommendationResult] = Field(default_factory=list)
    summary: str = ""


class ReportComparison(BaseModel):
    """Result from compare_reports tool."""

    status: str = "success"
    baseline_path: str
    comparison_path: str
    tps_change_pct: float = 0.0
    latency_change_pct: float = 0.0
    regressions: list[str] = Field(default_factory=list)
    improvements: list[str] = Field(default_factory=list)
    summary: str = ""
