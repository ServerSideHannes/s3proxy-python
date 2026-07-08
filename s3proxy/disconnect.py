"""Client disconnect errors for streaming uploads."""

from __future__ import annotations

from .errors import S3Error


class ClientDisconnectError(S3Error):
    """Client closed the connection mid-upload."""

    @classmethod
    def raised(cls) -> ClientDisconnectError:
        return cls(400, "BadRequest", "Client disconnected")
