"""Bounded credential-isolated S3 client pool owned by one application lifespan."""

import asyncio
from contextlib import asynccontextmanager

from ..errors import S3Error
from .s3 import S3Client


class S3ClientPool:
    def __init__(self, settings, max_clients=32):
        self.settings = settings
        self.max_clients = max_clients
        self.entries = {}
        self.condition = asyncio.Condition()
        self.closed = False

    @asynccontextmanager
    async def acquire(self, credentials):
        key = (credentials.access_key, credentials.secret_key, credentials.region)
        async with self.condition:
            if self.closed:
                raise S3Error.slow_down("S3 client pool is shutting down")
            if key not in self.entries:
                if len(self.entries) >= self.max_clients:
                    idle = next((k for k, (_, refs) in self.entries.items() if refs == 0), None)
                    if idle is None:
                        raise S3Error.slow_down("S3 client pool is busy")
                    client, _ = self.entries.pop(idle)
                    await client.__aexit__(None, None, None)
                client = S3Client(self.settings, credentials)
                await client.__aenter__()
                self.entries[key] = [client, 0]
            entry = self.entries[key]
            entry[1] += 1
        try:
            yield entry[0]
        finally:
            async with self.condition:
                entry[1] -= 1
                self.condition.notify_all()

    async def close(self):
        async with self.condition:
            self.closed = True
            await self.condition.wait_for(
                lambda: all(refs == 0 for _, refs in self.entries.values())
            )
            entries, self.entries = self.entries, {}
        for client, _ in entries.values():
            await client.__aexit__(None, None, None)
