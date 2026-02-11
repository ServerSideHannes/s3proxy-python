"""Multipart upload operations.

This package provides the MultipartHandlerMixin which combines:
- UploadPartMixin: streaming upload part handler
- LifecycleMixin: create, complete, abort operations
- ListPartsMixin: list parts handler
- CopyPartMixin: upload part copy handler
"""

from ..base import BaseHandler
from .copy import CopyPartMixin
from .lifecycle import LifecycleMixin
from .list import ListPartsMixin
from .upload_part import UploadPartMixin


class MultipartHandlerMixin(
    UploadPartMixin, LifecycleMixin, ListPartsMixin, CopyPartMixin, BaseHandler
):
    """Combined mixin for all multipart upload operations.

    Inherits from:
    - UploadPartMixin: handle_upload_part
    - LifecycleMixin: handle_create_multipart_upload, handle_complete_multipart_upload,
                      handle_abort_multipart_upload, _recover_upload_state
    - ListPartsMixin: handle_list_parts
    - CopyPartMixin: handle_upload_part_copy
    - BaseHandler: common utilities
    """

    pass


__all__ = [
    "CopyPartMixin",
    "LifecycleMixin",
    "ListPartsMixin",
    "MultipartHandlerMixin",
    "UploadPartMixin",
]
