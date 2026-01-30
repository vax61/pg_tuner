"""Config tools for pg_tuner MCP server.

Tools for reading PostgreSQL and OS configuration.
"""

import asyncio
import logging
from typing import Optional

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
import psycopg
from pydantic import BaseModel, Field

logger = logging.getLogger("pg_tuner_mcp.tools.config")

router = FastMCP("config")


class ConfigParam(BaseModel):
    """A PostgreSQL configuration parameter."""

    name: str
    setting: str
    unit: Optional[str] = None
    category: str = ""
    short_desc: str = ""
    context: str = ""  # postmaster, sighup, user, etc.
    pending_restart: bool = False


class PgConfigResult(BaseModel):
    """Result from get_current_config tool."""

    parameters: list[ConfigParam] = Field(default_factory=list)
    version: str = ""
    data_directory: str = ""


class SysctlParam(BaseModel):
    """A sysctl parameter."""

    name: str
    value: str


class SysctlConfigResult(BaseModel):
    """Result from get_sysctl_config tool."""

    parameters: list[SysctlParam] = Field(default_factory=list)


async def run_command(cmd: str, timeout: int = 60) -> str:
    """Run a shell command asynchronously with timeout."""
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
async def get_current_config(
    connection_string: str,
    category: Optional[str] = None,
) -> PgConfigResult:
    """Get current PostgreSQL configuration.

    Args:
        connection_string: PostgreSQL connection string.
        category: Optional category filter (e.g., 'Memory', 'WAL').

    Returns:
        Current PostgreSQL configuration parameters.
    """
    try:
        async with await psycopg.AsyncConnection.connect(connection_string) as conn:
            # Get version
            async with conn.cursor() as cur:
                await cur.execute("SELECT version()")
                row = await cur.fetchone()
                version = row[0] if row else ""

            # Get data directory
            async with conn.cursor() as cur:
                await cur.execute("SHOW data_directory")
                row = await cur.fetchone()
                data_directory = row[0] if row else ""

            # Get configuration parameters
            params = []
            async with conn.cursor() as cur:
                query = """
                    SELECT
                        name, setting, unit, category,
                        short_desc, context, pending_restart
                    FROM pg_settings
                """
                if category:
                    query += f" WHERE category ILIKE '%{category}%'"
                query += " ORDER BY category, name"

                await cur.execute(query)
                async for row in cur:
                    params.append(ConfigParam(
                        name=row[0],
                        setting=row[1],
                        unit=row[2],
                        category=row[3],
                        short_desc=row[4],
                        context=row[5],
                        pending_restart=row[6],
                    ))

            return PgConfigResult(
                parameters=params,
                version=version,
                data_directory=data_directory,
            )

    except psycopg.OperationalError as e:
        raise ToolError(f"Database connection failed: {e}")


@router.tool()
async def get_sysctl_config(
    prefix: str = "vm.",
) -> SysctlConfigResult:
    """Get relevant sysctl configuration parameters.

    Args:
        prefix: Sysctl namespace prefix to filter (e.g., 'vm.', 'kernel.').

    Returns:
        Sysctl parameters matching the prefix.
    """
    params = []

    # Key parameters for PostgreSQL tuning
    key_params = [
        "vm.swappiness",
        "vm.dirty_ratio",
        "vm.dirty_background_ratio",
        "vm.dirty_expire_centisecs",
        "vm.dirty_writeback_centisecs",
        "vm.overcommit_memory",
        "vm.overcommit_ratio",
        "vm.nr_hugepages",
        "kernel.shmmax",
        "kernel.shmall",
        "kernel.sem",
        "net.core.somaxconn",
        "net.ipv4.tcp_max_syn_backlog",
    ]

    for param in key_params:
        if param.startswith(prefix) or prefix == "":
            try:
                output = await run_command(f"sysctl -n {param} 2>/dev/null")
                params.append(SysctlParam(
                    name=param,
                    value=output.strip(),
                ))
            except ToolError:
                # Parameter may not exist on this system
                pass

    return SysctlConfigResult(parameters=params)
