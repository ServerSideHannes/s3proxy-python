"""In-flight request cap bounds httptools buffers outside the memory governor."""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from s3proxy.config import Settings
from s3proxy.disconnect import CHECK_INTERVAL_BYTES, ClientDisconnected, track_chunk


class TestResolvedMaxInFlight:
    def test_explicit_override(self):
        s = Settings(max_in_flight=12, memory_limit_mb=192, credentials=[])
        assert s.resolved_max_in_flight() == 12

    def test_auto_from_memory_limit(self):
        s = Settings(memory_limit_mb=192, credentials=[])
        assert s.resolved_max_in_flight() == 24

    def test_auto_minimum_four(self):
        s = Settings(memory_limit_mb=16, credentials=[])
        assert s.resolved_max_in_flight() == 4

    def test_unlimited_memory_means_unlimited_in_flight(self):
        s = Settings(memory_limit_mb=0, credentials=[])
        assert s.resolved_max_in_flight() is None


def test_main_passes_limit_concurrency_to_uvicorn():
    os.environ["S3PROXY_CREDENTIALS"] = '[{"access_key":"k","secret_key":"s","kek":"x"}]'
    with (
        patch("s3proxy.main.Settings") as settings_cls,
        patch("s3proxy.main.create_app"),
        patch("s3proxy.main.uvicorn.run") as run,
    ):
        settings = MagicMock()
        settings.ip = "0.0.0.0"
        settings.port = 4433
        settings.log_level = "INFO"
        settings.memory_limit_mb = 64
        settings.no_tls = True
        settings.resolved_max_in_flight.return_value = 8
        settings_cls.return_value = settings

        from s3proxy.main import main

        with patch("sys.argv", ["s3proxy"]):
            main()

    run.assert_called_once()
    assert run.call_args.kwargs["limit_concurrency"] == 8


@pytest.mark.asyncio
async def test_track_chunk_raises_when_client_disconnected():
    request = MagicMock()
    request.is_disconnected = AsyncMock(return_value=True)

    with pytest.raises(ClientDisconnected):
        await track_chunk(request, CHECK_INTERVAL_BYTES, 0)

    request.is_disconnected.assert_awaited_once()


@pytest.mark.asyncio
async def test_track_chunk_defers_check_until_interval():
    request = MagicMock()
    request.is_disconnected = MagicMock(return_value=False)

    remaining = await track_chunk(request, 1024, 0)
    assert remaining == 1024
    request.is_disconnected.assert_not_called()
