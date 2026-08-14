import asyncio
import subprocess
import sys
import textwrap
import threading

from lark_channel.core.cache import ExpiringCache
from lark_channel.ws import client as ws_client


def test_import_inside_running_loop_does_not_capture_caller_loop():
    code = textwrap.dedent(
        """
        import asyncio

        async def main():
            from lark_channel.ws import client as ws_client

            caller_loop = asyncio.get_running_loop()
            client = ws_client.Client("cli_test", "secret", auto_reconnect=False)

            async def no_op():
                return None

            client._connect = no_op
            client._ping_loop = no_op
            ws_client._select = no_op

            await caller_loop.run_in_executor(None, client.start)
            assert client._loop is not caller_loop
            assert client._loop.is_closed()

        asyncio.run(main())
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr


def test_clients_started_in_parallel_own_distinct_loops(monkeypatch):
    clients = [
        ws_client.Client("cli_one", "secret", auto_reconnect=False),
        ws_client.Client("cli_two", "secret", auto_reconnect=False),
    ]
    barrier = threading.Barrier(2)
    observed_loops = []
    errors = []

    async def connect_and_meet():
        observed_loops.append(asyncio.get_running_loop())
        barrier.wait(timeout=2)

    async def no_op():
        return None

    monkeypatch.setattr(ws_client, "_select", no_op)
    for client in clients:
        client._connect = connect_and_meet
        client._ping_loop = no_op

    def start(client):
        try:
            client.start()
        except Exception as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    threads = [threading.Thread(target=start, args=(client,)) for client in clients]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    assert len(observed_loops) == 2
    assert clients[0]._loop is not clients[1]._loop
    assert set(observed_loops) == {client._loop for client in clients}
    assert all(client._loop.is_closed() for client in clients)


def test_expiring_cache_binds_only_when_owner_loop_is_known():
    cache = ExpiringCache(clear_interval=0.01)

    assert cache._loop is None
    assert cache._cron is None

    owner_loop = asyncio.new_event_loop()
    cache.bind_loop(owner_loop)

    assert cache._loop is owner_loop
    assert cache._cron is not None
    assert not cache._cron.cancelled()

    cache.set("expired", "value", -1)
    owner_loop.run_until_complete(asyncio.sleep(0.02))
    assert "expired" not in cache._cache

    cache.close()
    owner_loop.run_until_complete(asyncio.sleep(0))
    owner_loop.close()
