# -*- coding: utf-8 -*-

from __future__ import annotations

import asyncio
import logging
import time

from typing import Any, Dict

from discord.backoff import ExponentialBackoff
from discord.errors import ConnectionClosed
from discord.voice_state import ConnectionFlowState, VoiceConnectionState

log = logging.getLogger(__name__)

__all__ = [
    'InstrumentedVoiceConnectionState',
]


class InstrumentedVoiceConnectionState(VoiceConnectionState):
    def _emit_transport_event(self, event: str, **payload: Any) -> None:
        voice_client = getattr(self, 'voice_client', None)
        callback = getattr(voice_client, '_handle_transport_state_event', None)
        if not callable(callback):
            return

        payload.setdefault('timestamp', time.time())
        payload.setdefault('monotonic', time.monotonic())
        payload.setdefault('state_name', getattr(self.state, 'name', 'unknown'))
        payload.setdefault('session_id', getattr(self, 'session_id', None))
        payload.setdefault('transport_epoch', getattr(voice_client, '_transport_epoch', 0))
        payload.setdefault('channel_id', getattr(getattr(voice_client, 'channel', None), 'id', None))
        payload.setdefault('guild_id', getattr(getattr(voice_client, 'guild', None), 'id', None))
        callback(event, payload)

    async def _poll_voice_ws(self, reconnect: bool) -> None:
        backoff = ExponentialBackoff()
        while True:
            try:
                await self.ws.poll_event()
            except asyncio.CancelledError:
                return
            except (ConnectionClosed, asyncio.TimeoutError) as exc:
                retry = None
                payload: Dict[str, Any] = {
                    'close_code': exc.code if isinstance(exc, ConnectionClosed) else None,
                    'reason': exc.reason if isinstance(exc, ConnectionClosed) else 'voice websocket timeout',
                    'is_timeout': isinstance(exc, asyncio.TimeoutError),
                    'exception_type': type(exc).__name__,
                    'reconnect_enabled': reconnect,
                }

                if isinstance(exc, ConnectionClosed):
                    if exc.code == 1000:
                        payload['reason'] = exc.reason or 'normal closure'
                        self._emit_transport_event('voice_transport_closed', **payload)
                        if not self._expecting_disconnect:
                            log.info('Disconnecting from voice normally, close code %d.', exc.code)
                            await self.disconnect()
                        break

                    if exc.code in (4014, 4022):
                        if self._disconnected.is_set():
                            payload['reason'] = exc.reason or 'discord voice disconnect'
                            self._emit_transport_event('voice_transport_closed', **payload)
                            log.info('Disconnected from voice by discord, close code %d.', exc.code)
                            await self.disconnect()
                            break

                        log.info('Disconnected from voice by force... potentially reconnecting.')
                        self._emit_transport_event('voice_transport_reconnect_scheduled', retry_delay=0.0, **payload)
                        successful = await self._potential_reconnect()
                        if not successful:
                            payload['reason'] = exc.reason or 'forced reconnect unsuccessful'
                            self._emit_transport_event('voice_transport_closed', **payload)
                            log.info('Reconnect was unsuccessful, disconnecting from voice normally...')
                            if self.state is not ConnectionFlowState.disconnected:
                                await self.disconnect()
                            break

                        self._emit_transport_event('voice_transport_reconnected', retry_delay=0.0, **payload)
                        continue

                    if exc.code == 4021:
                        payload['reason'] = exc.reason or 'voice rate limited'
                        self._emit_transport_event('voice_transport_closed', **payload)
                        log.warning('We are being ratelimited while trying to connect to voice. Disconnecting...')
                        if self.state is not ConnectionFlowState.disconnected:
                            await self.disconnect()
                        break

                    if exc.code == 4015:
                        log.info('Disconnected from voice, attempting a resume...')
                        self._emit_transport_event('voice_transport_reconnect_scheduled', retry_delay=0.0, **payload)
                        try:
                            await self._connect(
                                reconnect=reconnect,
                                timeout=self.timeout,
                                self_deaf=(self.self_voice_state or self).self_deaf,
                                self_mute=(self.self_voice_state or self).self_mute,
                                resume=True,
                            )
                        except asyncio.TimeoutError:
                            payload['reason'] = 'voice resume timed out'
                            self._emit_transport_event('voice_transport_closed', **payload)
                            log.info('Could not resume the voice connection... Disconnecting...')
                            if self.state is not ConnectionFlowState.disconnected:
                                await self.disconnect()
                            break

                        self._emit_transport_event('voice_transport_reconnected', retry_delay=0.0, **payload)
                        log.info('Successfully resumed voice connection')
                        continue

                    log.debug('Not handling close code %s (%s)', exc.code, exc.reason or 'no reason')

                if not reconnect:
                    self._emit_transport_event('voice_transport_closed', retry_delay=None, **payload)
                    await self.disconnect()
                    raise

                retry = backoff.delay()
                self._emit_transport_event('voice_transport_reconnect_scheduled', retry_delay=retry, **payload)
                log.exception('Disconnected from voice... Reconnecting in %.2fs.', retry)
                await asyncio.sleep(retry)
                await self.disconnect(cleanup=False)

                try:
                    await self._connect(
                        reconnect=reconnect,
                        timeout=self.timeout,
                        self_deaf=(self.self_voice_state or self).self_deaf,
                        self_mute=(self.self_voice_state or self).self_mute,
                        resume=False,
                    )
                except asyncio.TimeoutError:
                    payload['reason'] = 'voice reconnect timed out'
                    self._emit_transport_event('voice_transport_closed', retry_delay=retry, **payload)
                    log.warning('Could not connect to voice... Retrying...')
                    continue

                self._emit_transport_event('voice_transport_reconnected', retry_delay=retry, **payload)
