"""
speakispeak: real-time admin dashboard web server
  with all my heart, 2026, mark joshwel <mark@joshwel.co>
  SPDX-License-Identifier: Unlicense OR 0BSD
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

from aiohttp import web

if TYPE_CHECKING:
    pass

log = logging.getLogger(__name__)

WEB_DIR: Path = Path(__file__).resolve().parent.parent.joinpath("web")
DIST_DIR: Path = WEB_DIR.joinpath("dist")


class DashboardServer:
    """aiohttp server: serves the built React app and a /ws WebSocket endpoint.

    Thread-safe via ``make_emitter()``: the returned callable can be called from
    any thread and schedules a broadcast on the event loop.
    """

    def __init__(self, port: int) -> None:
        self._port = port
        self._clients: set[web.WebSocketResponse] = set()
        self._cached_session_state: dict[str, Any] | None = None
        self._app = web.Application()
        self._runner: web.AppRunner | None = None
        self._setup_routes()

    # ── Routes ────────────────────────────────────────────────────────────

    def _setup_routes(self) -> None:
        self._app.router.add_get("/ws", self._handle_ws)
        # Serve the built React dist if it exists; fall back to the src tree
        # so a bare `bun run dev` still forwards correctly via Vite proxy.
        static_root = DIST_DIR if DIST_DIR.exists() else None
        if static_root is not None:
            self._app.router.add_get("/", self._serve_index)
            self._app.router.add_get("/{tail:.*}", self._serve_static)
            log.info("speaki: dashboard: serving from %s", static_root)
        else:
            log.info(
                "speaki: dashboard: no dist/ build found — WebSocket only on port %s "
                "(run `bun run build` in web/ or use `bun run dev` for live reload)",
                self._port,
            )

    async def _serve_index(self, _request: web.Request) -> web.Response:
        index = DIST_DIR.joinpath("index.html")
        if not index.exists():
            return web.Response(status=404, text="build not found")
        return web.Response(
            body=index.read_bytes(),
            content_type="text/html",
            charset="utf-8",
        )

    async def _serve_static(self, request: web.Request) -> web.Response:
        tail = request.match_info.get("tail", "")
        target = DIST_DIR.joinpath(tail)
        if not target.exists() or not target.is_file():
            # SPA fallback
            return await self._serve_index(request)
        mime = _guess_mime(target)
        return web.Response(body=target.read_bytes(), content_type=mime)

    async def _handle_ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=20.0)
        await ws.prepare(request)
        self._clients.add(ws)
        log.debug("speaki: dashboard: ws client connected (%s total)", len(self._clients))

        if self._cached_session_state is not None:
            try:
                await ws.send_str(json.dumps(self._cached_session_state))
            except Exception:
                pass

        try:
            async for _msg in ws:
                pass  # no client→server messages expected
        finally:
            self._clients.discard(ws)
            log.debug("speaki: dashboard: ws client disconnected (%s total)", len(self._clients))

        return ws

    # ── Broadcasting ──────────────────────────────────────────────────────

    async def broadcast(self, event: dict[str, Any]) -> None:
        if not self._clients:
            return
        data = json.dumps(event, default=str)
        dead: set[web.WebSocketResponse] = set()
        for ws in list(self._clients):
            try:
                await ws.send_str(data)
            except Exception:
                dead.add(ws)
        self._clients -= dead

    def update_session_state(self, state: dict[str, Any]) -> None:
        """Cache the current session state for new WebSocket connections."""
        self._cached_session_state = state

    def make_emitter(self, loop: asyncio.AbstractEventLoop) -> Callable[[dict[str, Any]], None]:
        """Return a thread-safe emit function that can be called from any thread."""

        def emit(event: dict[str, Any]) -> None:
            loop.call_soon_threadsafe(loop.create_task, self.broadcast(event))

        return emit

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._runner = web.AppRunner(self._app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", self._port)
        await site.start()
        log.info("speaki: info: dashboard listening on http://0.0.0.0:%s/", self._port)

    async def stop(self) -> None:
        for ws in list(self._clients):
            try:
                await ws.close()
            except Exception:
                pass
        self._clients.clear()
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None


# ── Helpers ───────────────────────────────────────────────────────────────

_MIME: dict[str, str] = {
    ".html": "text/html",
    ".js": "application/javascript",
    ".mjs": "application/javascript",
    ".css": "text/css",
    ".json": "application/json",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".svg": "image/svg+xml",
    ".ico": "image/x-icon",
    ".woff": "font/woff",
    ".woff2": "font/woff2",
    ".ttf": "font/ttf",
    ".otf": "font/otf",
}


def _guess_mime(path: Path) -> str:
    return _MIME.get(path.suffix.lower(), "application/octet-stream")
