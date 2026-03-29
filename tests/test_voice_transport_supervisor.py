from __future__ import annotations

import asyncio
import time
import unittest
from collections import deque
from types import SimpleNamespace
from unittest.mock import Mock

from elias.vendor_bootstrap import bootstrap_voice_recv_vendor

bootstrap_voice_recv_vendor()

from elias.session import SpeakiSession
from elias.state import VOICE_HARD_RESET_RETRY_DELAY_SECONDS


def make_session() -> SpeakiSession:
    session = object.__new__(SpeakiSession)
    session._closed = False
    session.current_channel_id = 123
    session._self_session_id = "old-session"
    session._transport_epoch = 0
    session._recent_transport_reconnects = deque()
    session._post_reconnect_unstable_until_monotonic = 0.0
    session._last_dave_opcode_at = 0.0
    session._last_dave_opcode = -1
    session._last_transport_epoch_seen_by_dave = -1
    session._recovery_task = None
    session._queued_recovery_reason = None
    session._queued_recovery_hard = False
    session._schedule_recovery = Mock()
    session._recv_diag_snapshot = lambda *_args, **_kwargs: {}
    session._last_recv_diag = {}
    return session


class VoiceTransportSupervisorTests(unittest.IsolatedAsyncioTestCase):
    async def test_self_voice_state_update_tracks_session_epoch_and_channel(self) -> None:
        session = make_session()
        before = SimpleNamespace(channel=SimpleNamespace(id=123), session_id="old-session")
        after = SimpleNamespace(channel=SimpleNamespace(id=123), session_id="new-session")

        before_update = time.monotonic()
        await session.handle_self_voice_state_update(before, after)

        self.assertEqual(session.current_channel_id, 123)
        self.assertEqual(session._self_session_id, "new-session")
        self.assertEqual(session._transport_epoch, 1)
        self.assertGreater(session._post_reconnect_unstable_until_monotonic, before_update)
        session._schedule_recovery.assert_not_called()

    async def test_self_voice_state_update_schedules_hard_recovery_on_drop(self) -> None:
        session = make_session()
        before = SimpleNamespace(channel=SimpleNamespace(id=123), session_id="old-session")
        after = SimpleNamespace(channel=None, session_id="old-session")

        await session.handle_self_voice_state_update(before, after)

        session._schedule_recovery.assert_called_once()
        self.assertTrue(session._schedule_recovery.call_args.kwargs["hard"])

    async def test_reconnect_policy_allows_two_soft_attempts_then_hard_resets(self) -> None:
        session = make_session()

        first_attempts, first_hard = session._record_transport_reconnect_attempt(now=1.0, retry_delay=0.5)
        second_attempts, second_hard = session._record_transport_reconnect_attempt(now=2.0, retry_delay=0.5)
        third_attempts, third_hard = session._record_transport_reconnect_attempt(now=3.0, retry_delay=0.5)

        self.assertEqual(first_attempts, 1)
        self.assertFalse(first_hard)
        self.assertEqual(second_attempts, 2)
        self.assertFalse(second_hard)
        self.assertEqual(third_attempts, 3)
        self.assertTrue(third_hard)

    async def test_reconnect_policy_hard_resets_on_nonsensical_backoff(self) -> None:
        session = make_session()

        attempts, hard_reset = session._record_transport_reconnect_attempt(
            now=1.0,
            retry_delay=VOICE_HARD_RESET_RETRY_DELAY_SECONDS + 0.1,
        )

        self.assertEqual(attempts, 1)
        self.assertTrue(hard_reset)

    async def test_post_reconnect_decrypt_error_triggers_poison_reason(self) -> None:
        session = make_session()
        session._post_reconnect_unstable_until_monotonic = time.monotonic() + 5.0

        reason = session._poisoned_receive_reason(
            now=time.monotonic(),
            decrypt_error_delta=1,
            opus_decode_err_delta=0,
            pcm_frames_delta=0,
        )

        self.assertIsNotNone(reason)
        self.assertIn("decrypt+=1", reason)

    async def test_recent_dave_transition_marks_poison_reason(self) -> None:
        session = make_session()
        session._transport_epoch = 2
        session._last_dave_opcode = 22
        session._last_dave_opcode_at = time.monotonic()
        session._last_transport_epoch_seen_by_dave = 2
        session._post_reconnect_unstable_until_monotonic = time.monotonic() + 5.0

        reason = session._poisoned_receive_reason(
            now=time.monotonic(),
            decrypt_error_delta=0,
            opus_decode_err_delta=8,
            pcm_frames_delta=0,
        )

        self.assertIsNotNone(reason)
        self.assertIn("DAVE transport poison", reason)

    async def test_wait_for_active_recovery_task_blocks_until_completion(self) -> None:
        session = make_session()
        gate = asyncio.Event()
        resumed = False

        async def recovery() -> None:
            await gate.wait()

        session._recovery_task = asyncio.create_task(recovery())

        async def waiter() -> None:
            nonlocal resumed
            await session._wait_for_active_recovery_task()
            resumed = True

        wait_task = asyncio.create_task(waiter())
        await asyncio.sleep(0)
        self.assertFalse(resumed)

        gate.set()
        await wait_task
        await session._recovery_task
        self.assertTrue(resumed)


if __name__ == "__main__":
    unittest.main()
