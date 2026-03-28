"""
speakispeak: minimal discord-ext-voice-recv PCM-to-WAV test
  with all my heart, 2026, mark joshwel <mark@joshwel.co>
  SPDX-License-Identifier: Unlicense OR 0BSD
"""

from __future__ import annotations

import asyncio
import logging
import wave
from pathlib import Path
import tomllib

import discord

from elias.vendor_bootstrap import bootstrap_voice_recv_vendor

bootstrap_voice_recv_vendor()

from discord.ext import voice_recv


ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT_DIR.joinpath("config.toml")
OUTPUT_DIR = ROOT_DIR.joinpath("scripts").joinpath("wav_capture")
JOIN_TRIGGER = "wavtest"
LEAVE_TRIGGER = "wavstop"


def load_token() -> str:
    data = tomllib.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    token = data.get("app_token")
    if not isinstance(token, str) or not token:
        raise RuntimeError("recv_to_wav: error: missing app_token in config.toml")
    return token


def safe_name(value: str) -> str:
    cleaned = "".join(character if character.isalnum() else "_" for character in value).strip("_")
    return cleaned if cleaned else "unknown"


class PerUserWaveSink(voice_recv.AudioSink):
    def __init__(self, output_dir: Path):
        super().__init__()
        self.output_dir = output_dir
        self.wave_files: dict[int, wave.Wave_write] = {}
        self.paths: dict[int, Path] = {}
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def wants_opus(self) -> bool:
        return False

    def write(self, user: discord.User | discord.Member | None, data: voice_recv.VoiceData) -> None:
        if user is None or getattr(user, "bot", False):
            return

        pcm = data.pcm
        if not pcm:
            return

        wave_file = self.wave_files.get(user.id)
        if wave_file is None:
            filename = f"{user.id}_{safe_name(str(user))}.wav"
            path = self.output_dir.joinpath(filename)
            wave_file = wave.open(str(path), "wb")
            wave_file.setnchannels(discord.opus.Decoder.CHANNELS)
            wave_file.setsampwidth(discord.opus.Decoder.SAMPLE_SIZE // discord.opus.Decoder.CHANNELS)
            wave_file.setframerate(discord.opus.Decoder.SAMPLING_RATE)
            self.wave_files[user.id] = wave_file
            self.paths[user.id] = path
            logging.getLogger(__name__).info(
                "recv_to_wav: info: capturing %s to %s",
                user,
                path,
            )

        wave_file.writeframes(pcm)

    def cleanup(self) -> None:
        for wave_file in list(self.wave_files.values()):
            wave_file.close()
        self.wave_files.clear()
        self.paths.clear()


class RecvToWavClient(discord.Client):
    def __init__(self) -> None:
        intents = discord.Intents.none()
        intents.guilds = True
        intents.messages = True
        intents.message_content = True
        intents.voice_states = True
        super().__init__(intents=intents)
        self.voice_clients_by_guild: dict[int, voice_recv.VoiceRecvClient] = {}
        self.sinks_by_guild: dict[int, PerUserWaveSink] = {}

    async def on_ready(self) -> None:
        if self.user is not None:
            logging.info("recv_to_wav: info: logged in as %s (%s)", self.user, self.user.id)

    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot or message.guild is None:
            return

        content = message.content.strip().casefold()
        if content == JOIN_TRIGGER:
            await self._handle_join(message)
        elif content == LEAVE_TRIGGER:
            await self._handle_leave(message.guild.id)

    async def close(self) -> None:
        for guild_id in list(self.voice_clients_by_guild):
            await self._handle_leave(guild_id)
        await super().close()

    async def _handle_join(self, message: discord.Message) -> None:
        member = message.author if isinstance(message.author, discord.Member) else None
        if member is None or member.voice is None or member.voice.channel is None:
            await message.reply("You are not in a voice channel.", mention_author=False)
            return

        channel = member.voice.channel
        output_dir = OUTPUT_DIR.joinpath(f"guild_{message.guild.id}").joinpath(f"channel_{channel.id}")
        existing = self.voice_clients_by_guild.get(message.guild.id)

        if existing is not None and existing.is_connected():
            if existing.channel.id != channel.id:
                await existing.move_to(channel)
            if not existing.is_listening():
                sink = PerUserWaveSink(output_dir)
                self.sinks_by_guild[message.guild.id] = sink
                existing.listen(sink)
            await message.reply(f"Recording PCM WAVs to {output_dir}", mention_author=False)
            return

        voice_client = await channel.connect(cls=voice_recv.VoiceRecvClient)
        sink = PerUserWaveSink(output_dir)
        voice_client.listen(sink)
        self.voice_clients_by_guild[message.guild.id] = voice_client
        self.sinks_by_guild[message.guild.id] = sink
        logging.info(
            "recv_to_wav: info: joined %s#%s and recording to %s",
            channel.name,
            channel.id,
            output_dir,
        )
        await message.reply(f"Recording PCM WAVs to {output_dir}", mention_author=False)

    async def _handle_leave(self, guild_id: int) -> None:
        voice_client = self.voice_clients_by_guild.pop(guild_id, None)
        sink = self.sinks_by_guild.pop(guild_id, None)
        if voice_client is None:
            return

        if voice_client.is_listening():
            voice_client.stop_listening()
        if sink is not None:
            sink.cleanup()
        if voice_client.is_connected():
            await voice_client.disconnect(force=True)
        logging.info("recv_to_wav: info: stopped recording for guild %s", guild_id)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("discord.client").setLevel(logging.WARNING)
    logging.getLogger("discord.gateway").setLevel(logging.WARNING)
    logging.getLogger("discord.voice_state").setLevel(logging.WARNING)
    logging.getLogger("discord.ext.voice_recv.gateway").setLevel(logging.WARNING)
    logging.getLogger("discord.ext.voice_recv.reader").setLevel(logging.WARNING)
    logging.getLogger("discord.ext.voice_recv.router").setLevel(logging.WARNING)


def main() -> None:
    configure_logging()
    if not discord.opus.is_loaded():
        discord.opus._load_default()

    client = RecvToWavClient()
    try:
        client.run(load_token(), log_handler=None)
    except KeyboardInterrupt:
        logging.info("recv_to_wav: info: Ctrl+C received, shutting down")


if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    main()
