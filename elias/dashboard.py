"""
speakispeak: admin dashboard WebSocket server
  with all my heart, 2026, mark joshwel <mark@joshwel.co>
  SPDX-License-Identifier: Unlicense OR 0BSD
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from aiohttp import web

if TYPE_CHECKING:
    import discord
    from .session import SpeakiSession

log = logging.getLogger(__name__)

DASHBOARD_HTML = Path(__file__).resolve().parent.parent / "dashboard" / "index.html"
BROADCAST_INTERVAL = 0.5


class SpeakiDashboard:
    def __init__(self, client: Any) -> None:
        self._client = client
        self._app = web.Application()
        self._app.router.add_get("/", self._handle_index)
        self._app.router.add_get("/ws", self._handle_ws)
        self._runner: web.AppRunner | None = None
        self._ws_connections: set[web.WebSocketResponse] = set()
        self._broadcast_task: asyncio.Task[None] | None = None

    async def start(self, *, host: str, port: int) -> None:
        self._runner = web.AppRunner(self._app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, host, port)
        await site.start()
        self._broadcast_task = asyncio.create_task(self._run_broadcast_loop())
        log.info("speaki: info: dashboard listening on http://%s:%s", host, port)

    async def stop(self) -> None:
        if self._broadcast_task is not None:
            self._broadcast_task.cancel()
            try:
                await self._broadcast_task
            except asyncio.CancelledError:
                pass
            self._broadcast_task = None

        for ws in list(self._ws_connections):
            await ws.close()
        self._ws_connections.clear()

        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    async def _handle_index(self, request: web.Request) -> web.Response:
        if DASHBOARD_HTML.exists():
            return web.Response(
                text=DASHBOARD_HTML.read_text(encoding="utf-8"),
                content_type="text/html",
            )
        return web.Response(text="<h1>dashboard/index.html not found</h1>", content_type="text/html")

    async def _handle_ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self._ws_connections.add(ws)
        try:
            snapshot = self._collect_state()
            await ws.send_str(json.dumps(snapshot))
            async for _ in ws:
                pass  # Ignore client messages; push-only.
        finally:
            self._ws_connections.discard(ws)
        return ws

    async def _run_broadcast_loop(self) -> None:
        while True:
            await asyncio.sleep(BROADCAST_INTERVAL)
            if not self._ws_connections:
                continue
            try:
                payload = json.dumps(self._collect_state())
            except Exception:
                continue
            dead: set[web.WebSocketResponse] = set()
            for ws in list(self._ws_connections):
                try:
                    await ws.send_str(payload)
                except Exception:
                    dead.add(ws)
            self._ws_connections -= dead

    def _collect_state(self) -> dict[str, Any]:
        sessions_data: list[dict[str, Any]] = []
        client = self._client
        sessions: dict[int, Any] = getattr(client, "sessions", {})
        now_mono = time.monotonic()

        for guild_id, session in sessions.items():
            guild = session.guild
            channel_id = session.current_channel_id
            channel = guild.get_channel(channel_id) if channel_id else None
            channel_name = getattr(channel, "name", None)

            # Voice members in the channel.
            members_data: list[dict[str, Any]] = []
            sink = session.receive_sink
            for member in getattr(channel, "members", []):
                if member.bot:
                    continue
                speaking = False
                if sink is not None:
                    voice_until = sink._speaker_voice_until_monotonic.get(member.id, 0.0)
                    speaking = now_mono < voice_until
                transcription = session.recent_transcriptions.get(member.id)
                members_data.append({
                    "user_id": member.id,
                    "user_label": str(member),
                    "display_name": member.display_name,
                    "avatar_url": str(member.display_avatar.url) if member.display_avatar else None,
                    "speaking": speaking,
                    "transcription": transcription,
                })

            # Worker pool.
            workers_data: list[dict[str, Any]] = []
            for language, process in session.worker_processes.items():
                try:
                    q_size = session.worker_input_queues[language].qsize()
                except Exception:
                    q_size = 0
                workers_data.append({
                    "language": language,
                    "pid": process.pid,
                    "alive": process.is_alive(),
                    "queue_depth": q_size,
                    "active": q_size > 0,
                })

            sink_stats = {}
            if sink is not None:
                sink_stats = {
                    "dropped_chunks": sink.dropped_chunks,
                    "trimmed_buffers": sink.trimmed_buffers,
                }

            voice_connected = (
                session.voice_client is not None and session.voice_client.is_connected()
            )

            sessions_data.append({
                "guild_id": guild_id,
                "guild_name": guild.name,
                "channel_id": channel_id,
                "channel_name": channel_name,
                "voice_connected": voice_connected,
                "worker_enabled": session.worker_enabled,
                "enabled_languages": list(session.enabled_languages),
                "last_activity_ago": round(now_mono - session.last_activity_monotonic, 1),
                "members": members_data,
                "workers": workers_data,
                "sink": sink_stats,
            })

        bot_user = getattr(client, "user", None)
        return {
            "ts": time.time(),
            "bot": {
                "name": str(bot_user) if bot_user else None,
                "id": bot_user.id if bot_user else None,
            },
            "sessions": sessions_data,
        }
