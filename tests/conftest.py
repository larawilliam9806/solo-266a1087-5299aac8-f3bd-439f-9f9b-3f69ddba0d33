from __future__ import annotations

import asyncio
import socket
from contextlib import asynccontextmanager

import httpx
import pytest
import uvicorn

from app.collector import Collector
from app.config import Settings
from app.main import create_app
from mock_source.app import app as source_app, state as source_state


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class LiveServer:
    def __init__(self, app, port: int):
        self.app = app
        self.port = port
        self.server: uvicorn.Server | None = None
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        config = uvicorn.Config(
            self.app, host="127.0.0.1", port=self.port, log_level="warning", lifespan="on"
        )
        self.server = uvicorn.Server(config)
        self._task = asyncio.create_task(self.server.serve())
        for _ in range(200):
            if self.server.started:
                return
            if self._task.done():
                self._task.result()
            await asyncio.sleep(0.02)
        raise RuntimeError("server did not start")

    async def stop(self) -> None:
        if self.server:
            self.server.should_exit = True
        if self._task:
            await asyncio.wait_for(self._task, timeout=15)


@asynccontextmanager
async def run_server(app, port: int):
    server = LiveServer(app, port)
    await server.start()
    try:
        yield server
    finally:
        await server.stop()


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


class Stack:
    def __init__(self, source_url: str, api_url: str):
        self.source_url = source_url
        self.api_url = api_url


@pytest.fixture
async def stack(tmp_path):
    source_state.revisions.clear()
    source_state.delay_seconds = 0.0
    source_state.chunk_delay_seconds = 0.0
    source_state.failing.clear()
    source_state.corrupting.clear()
    source_state.no_range.clear()
    source_state.requests.clear()

    source_port = free_port()
    api_port = free_port()
    settings = Settings(
        source_base_url=f"http://127.0.0.1:{source_port}",
        data_dir=tmp_path / "data",
        http_timeout=15,
        chunk_size=8 * 1024,
        max_source_resyncs=3,
    )
    collector = Collector(settings)
    api_app = create_app(settings, collector)
    async with run_server(source_app, source_port):
        async with run_server(api_app, api_port):
            async with httpx.AsyncClient(timeout=30) as client:
                yield Stack(f"http://127.0.0.1:{source_port}", f"http://127.0.0.1:{api_port}"), client, settings, collector


async def set_delay(stack: Stack, client: httpx.AsyncClient, seconds: float, chunk: float = 0.0):
    await client.post(
        f"{stack.source_url}/admin/delay",
        json={"seconds": seconds, "chunk_seconds": chunk},
    )
