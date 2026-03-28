from __future__ import annotations

import asyncio
from pathlib import Path
import tomllib

import discord


CONFIG_PATH = Path(__file__).with_name("config.toml")


def load_config() -> tuple[str, int, str]:
    with CONFIG_PATH.open("rb") as config_file:
        config = tomllib.load(config_file)

    required_keys = (
        "app_token",
        "target_channel_id",
        "target_message_text",
    )
    missing_keys = [key for key in required_keys if not config.get(key)]
    if missing_keys:
        missing = ", ".join(missing_keys)
        raise ValueError(f"Missing required config.toml keys: {missing}")

    token = str(config["app_token"])
    channel_id = int(str(config["target_channel_id"]))
    message_text = str(config["target_message_text"])
    return token, channel_id, message_text


class MessageSenderClient(discord.Client):
    def __init__(self, channel_id: int, message_text: str) -> None:
        super().__init__(intents=discord.Intents.none())
        self.channel_id = channel_id
        self.message_text = message_text
        self._sent = False

    async def on_ready(self) -> None:
        if self._sent:
            return

        self._sent = True
        try:
            channel = self.get_channel(self.channel_id)
            if channel is None:
                channel = await self.fetch_channel(self.channel_id)

            if not isinstance(channel, discord.abc.Messageable):
                raise TypeError(
                    f"Channel {self.channel_id} does not support sending messages."
                )

            sent_message = await channel.send(self.message_text)
            print(
                "Message sent successfully:",
                f"channel_id={sent_message.channel.id}",
                f"message_id={sent_message.id}",
            )
        finally:
            await self.close()


async def main() -> None:
    token, channel_id, message_text = load_config()
    client = MessageSenderClient(channel_id=channel_id, message_text=message_text)
    await client.start(token)


if __name__ == "__main__":
    asyncio.run(main())
