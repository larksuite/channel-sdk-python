# Meeting channel

Agents that perceive and respond **inside a live meeting**: captions, meeting
chat, participants arriving and leaving, documents being shared.

Two entry points, one session type. Moving from one to the other changes the
entry-point line and nothing else.

| | `follow_my_meeting` — as the user | `join_meeting` — as the bot |
|---|---|---|
| Visible in the meeting | no | yes, a real participant |
| Credential | the user's own access token | the app's tenant token |
| How content arrives | polling `bots/events` | pushed `vc.bot.meeting_activity_v1` |
| Needs `connect()` | **no** — REST only | **yes** — activity is pushed |
| Can speak in the meeting | no, reply over IM | yes, `send_message` |
| Scope | `vc:meeting.meetingevent:read` | `vc:meeting.bot.join:write`, `vc:meeting.message:write` |

Both require the meeting's "allow agents to join" setting and Feishu client
7.68 or later. Joining as the bot is gated behind an application process.

## Joining a meeting

```python
channel = FeishuChannel(app_id=..., app_secret=...)

async def on_invited(invitation):
    session = await channel.join_meeting(invitation.meeting_no)

    def on_chat(event):
        if event.self_echo:
            return          # see "Echoes" below — this line is load-bearing
        ...

    session.on("chat", on_chat)

channel.on("meetingInvited", on_invited)
await channel.connect()
```

## Following a meeting

```python
session = await channel.follow_my_meeting(user_open_id="ou_...")
session.on("transcript", lambda e: notes.append(e.text))
```

No `connect()` needed. Read [Who the ticket belongs to](#who-the-ticket-belongs-to)
before wiring the `user_open_id`.

## Session events

`transcript` · `chat` · `participant` · `share` · `document_context` · `end` ·
`error`. `on()` is multicast and returns an unsubscribe.

```python
off = session.on("transcript", handler)
off()
```

`document_context` carries **identifiers only** — a comment id, an element
token. Fetching the comment body or the asset is your job, within the shared
document's temporary grant, with permissions you applied for.

### Ordering, and the price of it

Events are delivered **in order**, and each handler is awaited before the next
event. Order is the meaning of some of them: swapping the shared document
arrives as `magic_share_ended` then `magic_share_started`, and reordering makes
you reconstruct the wrong document.

The price:

- a handler that `await`s and takes a long time holds up **this meeting's**
  stream;
- a handler that blocks **without** awaiting (`time.sleep`, a synchronous HTTP
  call, heavy CPU) holds up **the entire process** — every meeting, the message
  path, the socket heartbeat. There is one thread. Hand blocking work to an
  executor.

### Captions

The protocol has no "final" marker, so a later item with the same
`sentence_id` supersedes the earlier text. Upsert on `sentence_id`.

`MeetingOptions(stabilize_seconds=...)` debounces instead: `0.0` (the default)
delivers every revision; a positive value delivers a sentence once, after it
stops changing for that long — at the cost of one window of latency, and of a
sentence never settling while somebody keeps talking.

### Echoes

The bot's own contributions come back around: an in-meeting message is pushed
back as meeting chat. Those arrive with `self_echo=True` and **are still
delivered** — a full record wants the bot's own turns.

So `if event.self_echo: return` is not boilerplate. Without it, a handler that
replies to chat replies to itself, at network speed. The SDK's backstop is
`meeting.send_rate_limit_per_minute` (default 20), after which `send_message`
raises `rate_limited`.

When the bot's own id is not yet known, `self_echo` is `True` — "maybe" has to
read as "yes", because `False` means "definitely not me" and would let the loop
close.

## Leaving versus disposing

| | Departs the meeting | Use for |
|---|---|---|
| `await session.leave()` | **yes** | you are done, the meeting ended |
| `session.dispose()` | **no** | reconnects, in-process cleanup |

`dispose()` deliberately does not depart: a reconnect must not make the bot
vanish from every meeting it is in. The flip side is that **the bot stays a
participant**, so:

- `channel.disconnect()` disposes sessions — it does not leave meetings;
- before the process exits, `leave()` every live session, or the bot sits in
  those meetings until the server ends them. `leave()` keeps working after
  `dispose()` precisely so this is possible.

Both are idempotent.

## Limits and reclamation

```python
FeishuChannel(
    app_id=..., app_secret=...,
    meeting=MeetingChannelConfig(
        max_concurrent_sessions=32,
        idle_timeout_seconds=0.0,
        liveness_probe_interval_seconds=300.0,
        send_rate_limit_per_minute=20,
    ),
)
```

Sessions are created **from outside** your process — joining starts at an
invitation from anybody who can add the bot to a meeting. Hence a ceiling,
shared by both entry points.

A seat is released when there is evidence the bot is no longer a participant:
a clean departure, a departure rejected because the meeting is gone, a
meeting-ended event, or a probe confirming absence. An inconclusive departure
(5xx, timeout) keeps the seat, and the next `join_meeting` /
`follow_my_meeting` retries it before comparing the ceiling.

`idle_timeout_seconds` is **off by default**: the liveness probe already
catches the bot being removed, so what is left for idle reclamation is a
meeting where nobody happens to be talking — and walking out of that one is
visible and wrong. Turning it on makes the session depart the meeting.

One caveat if you leave it off: idle reclamation is also the only backstop for
a wedged handler. **If your handlers can block for a long time, set a positive
value.**

## Diagnosing silence

Failures on this path are silent by nature — an undeclared subscription, a
missing permission and a renamed field all look like "nothing happened".

```python
health = channel.get_meeting_event_health()
```

| Symptom | Reading | Where to look |
|---|---|---|
| the type is absent from `stats` | the platform never sent it | meeting setting, subscription declaration, is the bot in the meeting |
| `empty == received` | nothing could be unpacked | field names or structure changed |
| `0 < empty < received` | some could not | an uncovered sub-type |
| `liveness.consecutive_unknown` climbing | the probe never concludes | its permission assumption may not hold in this tenant |
| `membership.held` never falling | seats are not coming back | `released_by_evidence` shows why |
| `dropped` climbing | a handler is slower than the meeting, so its session's delivery queue hit its ceiling | the handler — and move blocking work to an executor |
| `TOO_MANY_SESSIONS` while `membership.held` is `0` | the seat is held by a live session, not by server-side membership | `sessions` — `held` counts only the latter |

## When a handler cannot keep up

Delivery is serial and awaited, so a handler that yields but takes a long time
makes its session's queue grow at whatever rate the meeting produces activity.
That queue has a ceiling; past it, the newest event is refused and counted in
`dropped`.

The newest is refused rather than the oldest evicted, because order is meaning
here — a document swap arrives as `magic_share_ended` then
`magic_share_started`, and dropping from the front would split such a pair and
leave a queue that still looks complete. Refusing at the tail keeps what is
queued contiguous, so a gap is a gap at the end and `dropped` says so.

Error reports have their own headroom above the ceiling: they are what explain
why a session went away, and a ceiling full of transcripts must not be able to
drop the explanation.

## Untrusted content

`transcript.text`, `chat.content`, `topic`, `actor.name`, `doc.url`,
`doc.title` and the `document_context` sub-objects are written by meeting
participants, who may be external or guest users. Escape them before rendering
and never concatenate them into a log line.

Prompt injection is a residual risk that no SDK layer can remove — the whole
point of this feature is feeding meeting speech to a model. `actor` and
`self_echo` give you the minimum needed to tier trust; the rest is your call.

## Subscribing to unwrapped events

```python
off = channel.on_raw_event("vc.bot.meeting_started_v1", handler)
```

Different from the `"raw"` event, which mirrors already-wrapped events and is
controlled by `inbound.emit_raw_events`. This subscribes to types the channel
does not wrap, and ignores that switch.

The payload is authentic — this runs after signature verification and
decryption — but **unredacted**, and this path sits **outside the safety
pipeline**: no policy gate, no dedup, no processing lock, no loop guard.

> **Subscribing to a type the channel already handles opens an unpoliced path
> into that type.** With `dm_policy="allowlist"` set, a raw subscription to
> `im.message.receive_v1` still receives direct messages from everybody, and a
> redelivered event runs your handler again. That is what an escape hatch is —
> but know that you have opened one.

## Errors

`not_supported` (`send_message` while only following), `meeting_not_found`,
`too_many_sessions`, plus the existing `rate_limited`, `not_connected` and the
rest.

`FeishuChannelError.context` may carry `console_url` — a **signed one-click
authorization link**. Treat it as a credential: do not echo it into a chat, a
web page or a support ticket.

Failures inside a session go to the session's `error` event. With no handler
registered they are logged instead, minimally and without `context`.

## Who the ticket belongs to

`follow_my_meeting(user_open_id=...)` reads a meeting under **that user's**
authorization. The SDK receives a string; it cannot check whose it is, the
ticket store is shared across the process, and a cached ticket resolves
**without notifying its owner**.

So `user_open_id` must be somebody you have already established is the
requester. Passing a value taken from an inbound message means listening in on
someone else's meeting with their authorization, invisibly. `prompt_context`
must belong to the same person — pairing one person's `user_open_id` with
another's context sends the authorization card to the wrong person and files
the resulting ticket under the first.

`meeting.follow_allowlist` is the available gate; `meeting.invite_allowlist` is
its counterpart on the join side. Both default to open, because a closed
default would make the feature unusable out of the box.

See [Security configuration](security.md#meeting-channel) for the full picture.
