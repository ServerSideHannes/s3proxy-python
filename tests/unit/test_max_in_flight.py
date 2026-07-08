"""In-flight request cap bounds httptools buffers outside the memory governor."""

import os
from unittest.mock import MagicMock, patch

from s3proxy.config import Settings


class TestResolvedMaxInFlight:
    def test_explicit_override(self):
        s = Settings(max_in_flight=12, memory_limit_mb=192, credentials=[])
        assert s.resolved_max_in_flight() == 12

    def test_zero_means_unlimited(self):
        s = Settings(memory_limit_mb=192, credentials=[])
        assert s.resolved_max_in_flight() is None

    def test_unlimited_memory_still_unlimited_in_flight(self):
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
