"""Collectors tools for pg_tuner MCP server.

Tools for collecting PostgreSQL and OS metrics.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
import psycopg

from ..models.schemas import (
    PgStatsResult,
    BgwriterStats,
    StatementStats,
    OsMetricsResult,
    IostatResult,
    HwInfoResult,
)

logger = logging.getLogger("pg_tuner_mcp.tools.collectors")

router = FastMCP("collectors")


async def run_command(cmd: str, timeout: int = 60) -> str:
    """Run a shell command asynchronously with timeout.

    Args:
        cmd: Command to execute.
        timeout: Timeout in seconds.

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
        raise ToolError(f"Command timed out after {timeout}s: {cmd}")


@router.tool()
async def collect_pg_stats(
    connection_string: str,
    include_statements: bool = True,
    statements_limit: int = 20,
) -> PgStatsResult:
    """Collect PostgreSQL statistics from pg_stat_* views.

    Args:
        connection_string: PostgreSQL connection string.
        include_statements: Whether to include pg_stat_statements data.
        statements_limit: Max number of statements to return.

    Returns:
        PostgreSQL statistics including cache hit ratio and bgwriter stats.
    """
    try:
        async with await psycopg.AsyncConnection.connect(connection_string) as conn:
            # Get cache hit ratio
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT
                        CASE
                            WHEN blks_hit + blks_read = 0 THEN 0
                            ELSE blks_hit::float / (blks_hit + blks_read)
                        END as cache_hit_ratio
                    FROM pg_stat_database
                    WHERE datname = current_database()
                """)
                row = await cur.fetchone()
                cache_hit_ratio = row[0] if row else 0.0

            # Get bgwriter stats
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT
                        checkpoints_timed, checkpoints_req,
                        checkpoint_write_time, checkpoint_sync_time,
                        buffers_checkpoint, buffers_clean,
                        maxwritten_clean, buffers_backend,
                        buffers_backend_fsync, buffers_alloc
                    FROM pg_stat_bgwriter
                """)
                row = await cur.fetchone()
                bgwriter = BgwriterStats(
                    checkpoints_timed=row[0] or 0,
                    checkpoints_req=row[1] or 0,
                    checkpoint_write_time=row[2] or 0.0,
                    checkpoint_sync_time=row[3] or 0.0,
                    buffers_checkpoint=row[4] or 0,
                    buffers_clean=row[5] or 0,
                    maxwritten_clean=row[6] or 0,
                    buffers_backend=row[7] or 0,
                    buffers_backend_fsync=row[8] or 0,
                    buffers_alloc=row[9] or 0,
                ) if row else BgwriterStats()

            # Get top statements if requested
            statements = []
            if include_statements:
                async with conn.cursor() as cur:
                    try:
                        await cur.execute(f"""
                            SELECT
                                queryid, query, calls,
                                total_exec_time, mean_exec_time,
                                rows, shared_blks_hit, shared_blks_read
                            FROM pg_stat_statements
                            ORDER BY total_exec_time DESC
                            LIMIT {statements_limit}
                        """)
                        async for row in cur:
                            statements.append(StatementStats(
                                queryid=row[0],
                                query=row[1][:200],  # Truncate long queries
                                calls=row[2],
                                total_exec_time=row[3],
                                mean_exec_time=row[4],
                                rows=row[5],
                                shared_blks_hit=row[6],
                                shared_blks_read=row[7],
                            ))
                    except psycopg.errors.UndefinedTable:
                        logger.warning("pg_stat_statements extension not available")

            return PgStatsResult(
                timestamp=datetime.now(timezone.utc),
                cache_hit_ratio=cache_hit_ratio,
                bgwriter=bgwriter,
                statements=statements,
            )

    except psycopg.OperationalError as e:
        raise ToolError(f"Database connection failed: {e}")


@router.tool()
async def collect_os_metrics(
    device: str = "sda",
    interval: int = 1,
    count: int = 3,
) -> OsMetricsResult:
    """Collect OS metrics (iostat, memory, CPU).

    Args:
        device: Block device to monitor (e.g., sda, nvme0n1).
        interval: Sampling interval in seconds.
        count: Number of samples to average.

    Returns:
        OS metrics including I/O stats and memory/CPU usage.
    """
    iostat_result = None

    # Try to collect iostat
    try:
        output = await run_command(f"iostat -xd {device} {interval} {count}")
        # Parse last sample
        lines = output.strip().split("\n")
        for line in reversed(lines):
            parts = line.split()
            if parts and parts[0] == device:
                iostat_result = IostatResult(
                    device=device,
                    r_per_s=float(parts[1]) if len(parts) > 1 else 0.0,
                    w_per_s=float(parts[2]) if len(parts) > 2 else 0.0,
                    rkb_per_s=float(parts[3]) if len(parts) > 3 else 0.0,
                    wkb_per_s=float(parts[4]) if len(parts) > 4 else 0.0,
                    await_ms=float(parts[9]) if len(parts) > 9 else 0.0,
                    util_pct=float(parts[-1]) if len(parts) > 0 else 0.0,
                )
                break
    except ToolError:
        logger.warning(f"iostat not available for device {device}")

    # Get memory usage
    try:
        output = await run_command("free -m | grep Mem")
        parts = output.split()
        total = float(parts[1])
        used = float(parts[2])
        memory_used_pct = (used / total * 100) if total > 0 else 0.0
    except (ToolError, IndexError, ValueError):
        memory_used_pct = 0.0

    # Get CPU iowait
    try:
        output = await run_command("vmstat 1 2 | tail -1")
        parts = output.split()
        cpu_iowait_pct = float(parts[15]) if len(parts) > 15 else 0.0
    except (ToolError, IndexError, ValueError):
        cpu_iowait_pct = 0.0

    return OsMetricsResult(
        timestamp=datetime.now(timezone.utc),
        iostat=iostat_result,
        memory_used_pct=memory_used_pct,
        cpu_iowait_pct=cpu_iowait_pct,
    )


@router.tool()
async def collect_hw_info() -> HwInfoResult:
    """Collect hardware information (CPU, RAM, storage type).

    Returns:
        Hardware info including CPU cores, RAM, and storage type.
    """
    # Get CPU info
    try:
        output = await run_command("lscpu | grep -E '^CPU\\(s\\)|^Core|^Socket'")
        lines = output.strip().split("\n")
        cpu_logical = 1
        cores_per_socket = 1
        sockets = 1
        for line in lines:
            if line.startswith("CPU(s):"):
                cpu_logical = int(line.split(":")[1].strip())
            elif line.startswith("Core(s) per socket:"):
                cores_per_socket = int(line.split(":")[1].strip())
            elif line.startswith("Socket(s):"):
                sockets = int(line.split(":")[1].strip())
        cpu_physical = cores_per_socket * sockets
    except (ToolError, ValueError):
        cpu_logical = 1
        cpu_physical = 1

    # Get RAM
    try:
        output = await run_command("free -g | grep Mem")
        parts = output.split()
        ram_gb = float(parts[1])
    except (ToolError, IndexError, ValueError):
        ram_gb = 0.0

    # Detect storage type
    try:
        output = await run_command("lsblk -d -o NAME,ROTA | grep -v NAME | head -1")
        parts = output.split()
        if len(parts) >= 2:
            rotational = int(parts[1])
            storage_type = "HDD" if rotational == 1 else "SSD"
        else:
            storage_type = "unknown"
    except (ToolError, ValueError):
        storage_type = "unknown"

    # Check for NVMe
    try:
        await run_command("ls /dev/nvme0 2>/dev/null")
        storage_type = "NVMe"
    except ToolError:
        pass

    # Get NUMA nodes
    try:
        output = await run_command("lscpu | grep 'NUMA node(s)'")
        numa_nodes = int(output.split(":")[1].strip())
    except (ToolError, ValueError, IndexError):
        numa_nodes = 1

    return HwInfoResult(
        cpu_cores_physical=cpu_physical,
        cpu_cores_logical=cpu_logical,
        ram_gb=ram_gb,
        storage_type=storage_type,
        numa_nodes=numa_nodes,
    )
