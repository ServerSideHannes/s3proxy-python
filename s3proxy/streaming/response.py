"""Streaming responses whose resources also close on ASGI send failures."""

import anyio
from fastapi.responses import StreamingResponse


class OwnedStreamingResponse(StreamingResponse):
    def __init__(self, *args, cleanup=None, on_error=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.cleanup = cleanup
        self.on_error = on_error

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        except BaseException:
            if self.on_error is not None:
                self.on_error()
            raise
        finally:
            with anyio.CancelScope(shield=True):
                try:
                    if hasattr(self.body_iterator, "aclose"):
                        await self.body_iterator.aclose()
                finally:
                    if self.cleanup is not None:
                        await self.cleanup()
