"""The bot joins a meeting as a real participant, under the app's own token.

Run it, then add the bot to a meeting from the Feishu client. It answers
questions typed into the meeting chat and keeps a running transcript.

Needs ``vc:meeting.bot.join:write`` and ``vc:meeting.message:write``, the three
``vc.bot.*`` event subscriptions declared in the developer console, and the
meeting's "allow agents to join" setting turned on.
"""

import asyncio
import os

from lark_channel import FeishuChannel


def _require_env(name):
    value = os.environ.get(name)
    if not value:
        raise SystemExit(
            f"Missing {name}. Set it before running, for example: "
            f"export {name}=your_value"
        )
    return value


async def main():
    channel = FeishuChannel(
        app_id=_require_env("LARK_APP_ID"),
        app_secret=_require_env("LARK_APP_SECRET"),
    )

    async def on_invited(invitation):
        # `meetingInvited` is the only way into a joined meeting, and it does
        # not pass through the message policy: anybody who can add the bot to a
        # meeting can trigger this. Set `meeting.invite_allowlist` if only
        # certain people should be able to.
        session = await channel.join_meeting(invitation.meeting_no)
        transcript = {}

        def on_transcript(event):
            if event.self_echo:
                # Our own speech, transcribed back to us. Keeping it here is
                # deliberate — a full record wants the bot's turns too.
                return
            # A sentence id is an upsert handle, not a unique key: the platform
            # resends the same sentence as the speaker keeps talking.
            transcript[event.sentence_id] = event.text

        async def on_chat(event):
            if event.self_echo:
                # Without this the reply below arrives back as meeting chat and
                # the bot answers itself at network speed.
                return
            if not event.content.startswith("@assistant"):
                return
            question = event.content[len("@assistant"):].strip()
            answer = "%d sentences so far. You asked: %s" % (
                len(transcript),
                question,
            )
            await session.send_message(answer)

        session.on("transcript", on_transcript)
        session.on("chat", on_chat)
        session.on("participant", lambda e: print("%s %s" % (e.actor.name, e.action)))
        session.on("end", lambda e: print("meeting over: %s" % e.reason))
        # A slow handler holds up this meeting's stream, because delivery is
        # ordered. A handler that blocks *without* awaiting holds up the whole
        # process — hand blocking work to an executor.

    channel.on("meetingInvited", on_invited)
    try:
        await channel.connect()
    finally:
        # `dispose()` does not leave a meeting, so a plain shutdown leaves the
        # bot sitting in every meeting it joined until the server ends them.
        await channel.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
