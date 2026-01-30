"""Tests for pg_tuner_mcp tools."""

import json
import tempfile
from pathlib import Path

import pytest

from pg_tuner_mcp import __version__
from pg_tuner_mcp.models.schemas import (
    BurstReportAnalysis,
    SimulationReportAnalysis,
    ReportComparison,
    WorkloadResult,
)


class TestVersion:
    """Tests for version information."""

    def test_version_is_string(self):
        """Version should be a string."""
        assert isinstance(__version__, str)

    def test_version_format(self):
        """Version should follow semver format."""
        parts = __version__.split(".")
        assert len(parts) >= 2
        # Major and minor should be numeric
        assert parts[0].isdigit()
        assert parts[1].isdigit()


class TestModels:
    """Tests for Pydantic models."""

    def test_workload_result_model(self):
        """WorkloadResult model should work correctly."""
        result = WorkloadResult(
            mode="burst",
            duration_seconds=60.0,
            total_transactions=10000,
            tps=166.67,
            avg_latency_ms=5.0,
            p50_latency_ms=4.0,
            p95_latency_ms=8.0,
            p99_latency_ms=12.0,
        )
        assert result.mode == "burst"
        assert result.tps == 166.67

    def test_burst_report_analysis_model(self):
        """BurstReportAnalysis model should work correctly."""
        analysis = BurstReportAnalysis(
            status="success",
            report_path="/test/report.json",
            summary="Test analysis",
        )
        assert analysis.status == "success"
        assert analysis.report_path == "/test/report.json"

    def test_simulation_report_analysis_model(self):
        """SimulationReportAnalysis model should work correctly."""
        analysis = SimulationReportAnalysis(
            status="success",
            report_path="/test/sim_report.json",
            timeline_path="/test/timeline.csv",
            timeline_points=100,
        )
        assert analysis.timeline_points == 100

    def test_report_comparison_model(self):
        """ReportComparison model should work correctly."""
        comparison = ReportComparison(
            status="success",
            baseline_path="/test/baseline.json",
            comparison_path="/test/comparison.json",
            tps_change_pct=15.5,
            latency_change_pct=-10.0,
            improvements=["TPS increased by 15.5%"],
        )
        assert comparison.tps_change_pct == 15.5
        assert len(comparison.improvements) == 1


class TestServerTools:
    """Tests for server tool registration."""

    @pytest.mark.asyncio
    async def test_list_tools_returns_expected_tools(self):
        """Server should list expected tools."""
        from pg_tuner_mcp.server import mcp

        tools = await mcp.get_tools()
        tool_names = list(tools.keys())

        assert "ping" in tool_names
        assert "collect_pg_stats" in tool_names
        assert "collect_os_metrics" in tool_names
        assert "collect_hw_info" in tool_names
        assert "get_current_config" in tool_names
        assert "get_sysctl_config" in tool_names
        assert "run_workload" in tool_names
        assert "analyze_burst_report" in tool_names
        assert "analyze_simulation_report" in tool_names
        assert "compare_reports" in tool_names

    @pytest.mark.asyncio
    async def test_tools_count(self):
        """Server should have 10 tools."""
        from pg_tuner_mcp.server import mcp

        tools = await mcp.get_tools()
        assert len(tools) == 10


class TestWorkloadTools:
    """Tests for workload analysis tools."""

    @pytest.mark.asyncio
    async def test_analyze_burst_report_with_valid_file(self):
        """analyze_burst_report should work with valid JSON."""
        from pg_tuner_mcp.tools.workload import analyze_burst_report_impl

        # Create a minimal valid report
        report_data = {
            "mode": "burst",
            "summary": {
                "actual_duration_seconds": 60.0,
                "total_transactions": 10000,
                "tps": 166.67,
                "avg_latency_ms": 5.0,
                "p50_latency_ms": 4.0,
                "p95_latency_ms": 8.0,
                "p99_latency_ms": 12.0,
                "errors": 0,
            }
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(report_data, f)
            temp_path = f.name

        try:
            result = await analyze_burst_report_impl(temp_path)
            assert result.status == "success"
            assert result.workload is not None
            assert result.workload.tps == 166.67
        finally:
            Path(temp_path).unlink()

    @pytest.mark.asyncio
    async def test_analyze_burst_report_with_missing_file(self):
        """analyze_burst_report should raise error for missing file."""
        from pg_tuner_mcp.tools.workload import analyze_burst_report_impl
        from fastmcp.exceptions import ToolError

        with pytest.raises(ToolError, match="not found"):
            await analyze_burst_report_impl("/nonexistent/report.json")

    @pytest.mark.asyncio
    async def test_compare_reports_with_valid_files(self):
        """compare_reports should work with valid JSON files."""
        from pg_tuner_mcp.tools.workload import compare_reports_impl

        baseline_data = {
            "summary": {
                "actual_duration_seconds": 60.0,
                "total_transactions": 10000,
                "tps": 166.67,
                "avg_latency_ms": 5.0,
                "p50_latency_ms": 4.0,
                "p95_latency_ms": 8.0,
                "p99_latency_ms": 12.0,
                "errors": 0,
            }
        }

        comparison_data = {
            "summary": {
                "actual_duration_seconds": 60.0,
                "total_transactions": 12000,
                "tps": 200.0,  # 20% improvement
                "avg_latency_ms": 4.0,  # 20% improvement
                "p50_latency_ms": 3.5,
                "p95_latency_ms": 7.0,
                "p99_latency_ms": 10.0,
                "errors": 0,
            }
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(baseline_data, f)
            baseline_path = f.name

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(comparison_data, f)
            comparison_path = f.name

        try:
            result = await compare_reports_impl(baseline_path, comparison_path)
            assert result.status == "success"
            assert result.tps_change_pct > 0  # TPS improved
            assert result.latency_change_pct < 0  # Latency decreased
            assert len(result.improvements) > 0
        finally:
            Path(baseline_path).unlink()
            Path(comparison_path).unlink()
