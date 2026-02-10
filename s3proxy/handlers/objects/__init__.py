"""Object operations: GET, PUT, HEAD, DELETE, COPY, Tagging.

This package provides the ObjectHandlerMixin which combines:
- GetObjectMixin: GET object with encryption support
- PutObjectMixin: PUT object with encryption support
- MiscObjectMixin: HEAD, DELETE, COPY, tagging operations
"""

from ..base import BaseHandler
from .get import GetObjectMixin
from .misc import MiscObjectMixin
from .put import PutObjectMixin


class ObjectHandlerMixin(GetObjectMixin, PutObjectMixin, MiscObjectMixin, BaseHandler):
    """Combined mixin for all object operations."""

    pass


__all__ = [
    "GetObjectMixin",
    "MiscObjectMixin",
    "ObjectHandlerMixin",
    "PutObjectMixin",
]
