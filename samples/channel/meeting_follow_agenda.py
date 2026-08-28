"""Follow a meeting under the user's own token, without joining it.

The bot never appears in the meeting. It reads the meeting a user is already
in, under that user's own authorization, and nudges them over IM when the
discussion drifts off the agenda.

Needs ``vc:meeting.meetingevent:read`` on a user access token. No socket is
required — this path is REST only, so ``connect()`` is not called.

**Compliance.** This reads every participant's speech for the whole meeting
while the bot is invisible. Telling participants and getting their consent is
your responsibility. Note also that what the platform grants is whatever your
app applied for, which is usually much broader than meeting reads, and the
ticket is stored per user for the whole process to reuse.
"""

import asyncio
import os

from lark_channel import FeishuChannel
from lark_channel.channel.auth import FileTokenStore


def _require_env(name):
    value = os.environ.get(name)
    if not value:
        raise SystemExit(
            f"Missing {name}. Set it before running, for example: "
            f"export {name}=your_value"
        )
    return value


AGENDA = ["progress update", "risks", "next week"]


async def main():
    channel = FeishuChannel(
        app_id=_require_env("LARK_APP_ID"),
        app_secret=_require_env("LARK_APP_SECRET"),
        # Development only — it stores tickets as plaintext JSON. In production
        # implement TokenStore against your own secret manager.
        token_store=FileTokenStore("./.uat-tickets.json"),
    )

    # Must be the person who asked for this, not a value taken from an inbound
    # message: the SDK cannot tell the difference, and a cached ticket resolves
    # without notifying its owner.
    user_open_id = _require_env("LARK_USER_OPEN_ID")

    session = await channel.follow_my_meeting(user_open_id=user_open_id)
    print("following meeting %s (%s)" % (session.meeting_no, session.mode))

    recent = []

    def on_transcript(event):
        if event.self_echo:
            return
        recent.append("%s: %s" % (event.actor.name, event.text))
        del recent[:-200]

    session.on("transcript", on_transcript)
    session.on("end", lambda e: print("session over: %s" % e.reason))

    try:
        while True:
            await asyncio.sleep(60)
            if not recent:
                continue
            print("agenda %s / last %d lines" % (AGENDA, len(recent)))
    finally:
        await session.leave()


if __name__ == "__main__":
    asyncio.run(main())
