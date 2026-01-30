"""Workload tools for pg_tuner MCP server.

Tools for running pg_workload and analyzing reports.
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import Optional

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from ..models.schemas import (
    WorkloadResult,
    BurstReportAnalysis,
    SimulationReportAnalysis,
    ReportComparison,
    RecommendationResult,
)

logger = logging.getLogger("pg_tuner_mcp.tools.workload")

router = FastMCP("workload")


async def run_command(cmd: str, timeout: int = 600) -> str:
    """Run a shell command asynchronously with timeout.

    Args:
        cmd: Command to execute.
        timeout: Timeout in seconds (default 10 minutes for workloads).

    Returns:
        Command stdout.

    Raises:
        ToolError: If command fails or times out.
    """
    try:
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout)
        if proc.returncode != 0:
            raise ToolError(f"Command failed: {stderr.decode().strip()}")
        return stdout.decode()
    except asyncio.TimeoutError:
        raise ToolError(f"Command timed out after {timeout}s")


def parse_workload_report(data: dict) -> WorkloadResult:
    """Parse pg_workload JSON report into WorkloadResult."""
    # Handle both burst and simulation report formats
    summary = data.get("summary", data)

    return WorkloadResult(
        mode=data.get("mode", "burst"),
        duration_seconds=summary.get("actual_duration_seconds", 0),
        total_transactions=summary.get("total_transactions", 0),
        tps=summary.get("tps", 0.0),
        avg_latency_ms=summary.get("avg_latency_ms", 0.0),
        p50_latency_ms=summary.get("p50_latency_ms", 0.0),
        p95_latency_ms=summary.get("p95_latency_ms", 0.0),
        p99_latency_ms=summary.get("p99_latency_ms", 0.0),
        errors=summary.get("errors", 0),
    )


@router.tool()
async def run_workload(
    connection_string: str,
    profile: str = "oltp",
    duration: str = "60s",
    workers: int = 4,
    connections: int = 10,
    pg_workload_path: str = "pg_workload",
) -> WorkloadResult:
    """Run pg_workload benchmark against a PostgreSQL database.

    Args:
        connection_string: PostgreSQL connection string.
        profile: Workload profile (e.g., 'oltp', 'read_heavy').
        duration: Test duration (e.g., '60s', '5m').
        workers: Number of concurrent workers.
        connections: Number of database connections.
        pg_workload_path: Path to pg_workload binary.

    Returns:
        Workload execution results.
    """
    import tempfile

    # Create temp file for output
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        output_path = f.name

    try:
        # Build command
        cmd = (
            f"{pg_workload_path} run "
            f"--dsn '{connection_string}' "
            f"--duration {duration} "
            f"--workers {workers} "
            f"--connections {connections} "
            f"--output {output_path}"
        )

        logger.info(f"Running pg_workload: {cmd}")
        await run_command(cmd, timeout=3600)  # 1 hour max

        # Read and parse output
        with open(output_path) as f:
            data = json.load(f)

        return parse_workload_report(data)

    except FileNotFoundError:
        raise ToolError(f"pg_workload binary not found at: {pg_workload_path}")
    finally:
        # Cleanup temp file
        Path(output_path).unlink(missing_ok=True)


async def analyze_burst_report_impl(report_path: str) -> BurstReportAnalysis:
    """Implementation of burst report analysis.

    This function contains the actual logic and can be tested directly.
    """
    path = Path(report_path)
    if not path.exists():
        raise ToolError(f"Report file not found: {report_path}")

    try:
        with open(path) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ToolError(f"Invalid JSON in report file: {e}")

    workload = parse_workload_report(data)
    recommendations = []
    summary_points = []

    # Analyze results and generate recommendations
    if workload.p99_latency_ms > 100:
        recommendations.append(RecommendationResult(
            category="performance",
            parameter="work_mem",
            current_value="4MB",
            suggested_value="64MB",
            confidence="medium",
            restart_required=False,
            evidence=[f"P99 latency {workload.p99_latency_ms:.1f}ms > 100ms threshold"],
            impact="May reduce sort/hash spills to disk",
            risk="Increases per-connection memory usage",
        ))
        summary_points.append(f"High P99 latency ({workload.p99_latency_ms:.1f}ms)")

    if workload.errors > 0:
        error_rate = workload.errors / max(workload.total_transactions, 1) * 100
        summary_points.append(f"Error rate: {error_rate:.2f}%")
        if error_rate > 1:
            recommendations.append(RecommendationResult(
                category="connections",
                parameter="max_connections",
                current_value="100",
                suggested_value="200",
                confidence="low",
                restart_required=True,
                evidence=[f"Error rate {error_rate:.2f}% may indicate connection exhaustion"],
                impact="Allows more concurrent connections",
                risk="Increases memory overhead per connection",
            ))

    tps = workload.tps
    if tps > 0:
        summary_points.append(f"Throughput: {tps:.0f} TPS")

    return BurstReportAnalysis(
        status="success",
        report_path=report_path,
        workload=workload,
        recommendations=recommendations,
        summary="; ".join(summary_points) if summary_points else "Analysis complete",
    )


@router.tool()
async def analyze_burst_report(report_path: str) -> BurstReportAnalysis:
    """Analyze a pg_workload burst mode report and provide tuning recommendations.

    Args:
        report_path: Path to the JSON report file generated by pg_workload.

    Returns:
        Analysis results with recommendations.
    """
    return await analyze_burst_report_impl(report_path)


@router.tool()
async def analyze_simulation_report(
    report_path: str,
    timeline_path: Optional[str] = None,
) -> SimulationReportAnalysis:
    """Analyze a pg_workload simulation mode report with time-series data.

    Args:
        report_path: Path to the JSON report file generated by pg_workload simulate.
        timeline_path: Optional path to the CSV timeline file.

    Returns:
        Analysis results with time-series insights and recommendations.
    """
    path = Path(report_path)
    if not path.exists():
        raise ToolError(f"Report file not found: {report_path}")

    try:
        with open(path) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ToolError(f"Invalid JSON in report file: {e}")

    workload = parse_workload_report(data)
    recommendations = []
    summary_points = []

    # Count timeline points if available
    timeline_points = 0
    if timeline_path:
        timeline = Path(timeline_path)
        if timeline.exists():
            with open(timeline) as f:
                timeline_points = sum(1 for _ in f) - 1  # Exclude header

    # Analyze simulation-specific metrics
    if "timeline" in data:
        timeline_data = data["timeline"]
        # Look for patterns in timeline data
        if isinstance(timeline_data, list) and len(timeline_data) > 0:
            # Check for latency spikes
            latencies = [p.get("p99_latency_ms", 0) for p in timeline_data if "p99_latency_ms" in p]
            if latencies:
                max_latency = max(latencies)
                avg_latency = sum(latencies) / len(latencies)
                if max_latency > avg_latency * 3:
                    recommendations.append(RecommendationResult(
                        category="wal",
                        parameter="checkpoint_completion_target",
                        current_value="0.5",
                        suggested_value="0.9",
                        confidence="medium",
                        restart_required=False,
                        evidence=[f"Latency spikes detected: max {max_latency:.1f}ms vs avg {avg_latency:.1f}ms"],
                        impact="Spreads checkpoint I/O over longer period",
                        risk="Slightly longer recovery time after crash",
                    ))

    summary_points.append(f"Simulated duration: {workload.duration_seconds:.0f}s")
    if timeline_points > 0:
        summary_points.append(f"Timeline points: {timeline_points}")
    summary_points.append(f"Avg TPS: {workload.tps:.0f}")

    return SimulationReportAnalysis(
        status="success",
        report_path=report_path,
        timeline_path=timeline_path,
        workload=workload,
        timeline_points=timeline_points,
        recommendations=recommendations,
        summary="; ".join(summary_points),
    )


async def compare_reports_impl(
    baseline_path: str,
    comparison_path: str,
) -> ReportComparison:
    """Implementation of report comparison.

    This function contains the actual logic and can be tested directly.
    """
    baseline = Path(baseline_path)
    comparison = Path(comparison_path)

    if not baseline.exists():
        raise ToolError(f"Baseline report not found: {baseline_path}")
    if not comparison.exists():
        raise ToolError(f"Comparison report not found: {comparison_path}")

    try:
        with open(baseline) as f:
            baseline_data = json.load(f)
        with open(comparison) as f:
            comparison_data = json.load(f)
    except json.JSONDecodeError as e:
        raise ToolError(f"Invalid JSON in report file: {e}")

    baseline_workload = parse_workload_report(baseline_data)
    comparison_workload = parse_workload_report(comparison_data)

    # Calculate changes
    tps_change = 0.0
    if baseline_workload.tps > 0:
        tps_change = ((comparison_workload.tps - baseline_workload.tps) /
                      baseline_workload.tps * 100)

    latency_change = 0.0
    if baseline_workload.avg_latency_ms > 0:
        latency_change = ((comparison_workload.avg_latency_ms - baseline_workload.avg_latency_ms) /
                          baseline_workload.avg_latency_ms * 100)

    regressions = []
    improvements = []

    # TPS changes
    if tps_change < -5:
        regressions.append(f"TPS decreased by {abs(tps_change):.1f}%")
    elif tps_change > 5:
        improvements.append(f"TPS increased by {tps_change:.1f}%")

    # Latency changes
    if latency_change > 10:
        regressions.append(f"Latency increased by {latency_change:.1f}%")
    elif latency_change < -10:
        improvements.append(f"Latency decreased by {abs(latency_change):.1f}%")

    # Error rate changes
    baseline_errors = baseline_workload.errors
    comparison_errors = comparison_workload.errors
    if comparison_errors > baseline_errors * 1.5 and comparison_errors > 10:
        regressions.append(f"Errors increased from {baseline_errors} to {comparison_errors}")
    elif comparison_errors < baseline_errors * 0.5 and baseline_errors > 10:
        improvements.append(f"Errors decreased from {baseline_errors} to {comparison_errors}")

    # Generate summary
    if regressions and not improvements:
        summary = f"Performance regression detected: {'; '.join(regressions)}"
    elif improvements and not regressions:
        summary = f"Performance improved: {'; '.join(improvements)}"
    elif regressions and improvements:
        summary = "Mixed results with both improvements and regressions"
    else:
        summary = "No significant performance changes detected"

    return ReportComparison(
        status="success",
        baseline_path=baseline_path,
        comparison_path=comparison_path,
        tps_change_pct=tps_change,
        latency_change_pct=latency_change,
        regressions=regressions,
        improvements=improvements,
        summary=summary,
    )


@router.tool()
async def compare_reports(
    baseline_path: str,
    comparison_path: str,
) -> ReportComparison:
    """Compare two pg_workload reports to identify performance changes.

    Args:
        baseline_path: Path to the baseline report JSON file.
        comparison_path: Path to the comparison report JSON file.

    Returns:
        Comparison results with regressions and improvements.
    """
    return await compare_reports_impl(baseline_path, comparison_path)
