"""Running work on the loop that owns it.

A meeting session owns an ``asyncio.Queue``, a set of tasks and a few timer
handles. All three are bound to one loop: a queue whose consumer is on loop A
never wakes for a producer on loop B, ``Task.cancel()`` from another loop is not
safe, and a ``TimerHandle`` belongs to the loop that scheduled it.

The public surface — ``join_meeting``, ``follow_my_meeting``, ``dispose()``,
``leave()`` — can be called from any loop. These two helpers are the single
place that reconciles those two facts, so the policy (including what to do with
a loop that has already stopped) is written once instead of three times.
"""

import asyncio
from typing import Any, Callable, Optional


def _current_loop() -> Optional[Any]:
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return None


def run_on(loop: Optional[Any], fn: Callable[[], None]) -> None:
    """Run ``fn`` on ``loop``, from a synchronous caller.

    Falls back to running inline when there is no loop, when we are already on
    it, or when it has stopped — teardown has to make progress even after the
    loop it was using is gone.
    """
    if loop is None or _current_loop() is loop or not loop.is_running():
        fn()
        return
    try:
        loop.call_soon_threadsafe(fn)
    except RuntimeError:  # pragma: no cover - closed between the check and here
        fn()


async def await_on(loop: Optional[Any], factory: Callable[[], Any]) -> Any:
    """Await ``factory()`` on ``loop``, from an asynchronous caller.

    The stopped-loop case matters: ``disconnect()`` stops the background loop,
    and the documented shutdown order is "disconnect, then leave the meetings
    you are still in". Handing that coroutine to a stopped loop would wait
    forever, so it runs on the caller's loop instead — the session's timers and
    tasks are already gone by then, and what is left is a REST call.
    """
    if loop is None or _current_loop() is loop or not loop.is_running():
        return await factory()
    try:
        future = asyncio.run_coroutine_threadsafe(factory(), loop)
    except RuntimeError:  # pragma: no cover - closed between the check and here
        return await factory()
    return await asyncio.wrap_future(future)


__all__ = ["await_on", "run_on"]
