from __future__ import annotations

import asyncio
import socket
from contextlib import asynccontextmanager

import pytest
import uvicorn


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
