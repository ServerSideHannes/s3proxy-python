"""One interpretation of a backend object for reads, conditions, lists and copies."""

from dataclasses import dataclass

from .models import MultipartMetadata


@dataclass(frozen=True, slots=True)
class ObjectDescriptor:
    head: dict
    multipart: MultipartMetadata | None
    plaintext_size: int
    etag: str

    @property
    def generation(self) -> str:
        return self.multipart.generation if self.multipart else ""
