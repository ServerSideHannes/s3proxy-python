"""Placement of client parts in an upload's internal S3 part-number space.

S3 assembles the object in ascending part-number order and reads assume the
ciphertext of client part N starts after that of every lower client part. So
the internal ranges of an upload must be disjoint and ordered like the client
parts, however the parts arrive. A part is placed in the free window between
its already-placed neighbours; the window is also kept large enough for every
unplaced part between them to get at least one number.
"""

from __future__ import annotations

from collections.abc import Mapping

from ..crypto import MAX_INTERNAL_PARTS_PER_CLIENT, S3_MAX_PART_NUMBER, internal_part_range

# While every part so far fits one internal part, the first part to arrive may
# be the short last part of an upload whose other parts need several. Reserve
# sparse slots for the parts below it so those can still be placed. The unused
# numbers are lost for good, so this is capped at 19 per part below it
# (at most 285 of S3's 10000) to leave room for very large single-internal uploads.
AMBIGUOUS_FIRST_PART_MAX = 16


class PartPlacementError(ValueError):
    """No placement keeps this client part ordered and disjoint from the others."""


def place_client_part(
    allocations: Mapping[int, tuple[int, int]],
    client_part: int,
    count: int,
    *,
    single_internal: bool,
) -> tuple[int, int]:
    """Return the inclusive internal range for ``client_part`` needing ``count`` parts.

    ``allocations`` maps already-placed client parts to their ranges.
    ``single_internal`` is True while every part so far fits one internal part:
    placement is then dense (internal N for client N), as 600-part uploads need
    to stay under S3's 10000-part limit. Otherwise it is the sparse layout
    (20 numbers per client part). Either preference is dropped for a compact
    placement when it would collide; if no placement is safe, raise.
    """
    if not 1 <= client_part <= S3_MAX_PART_NUMBER:
        raise PartPlacementError(f"client part {client_part} is outside 1-{S3_MAX_PART_NUMBER}")
    if not 1 <= count <= MAX_INTERNAL_PARTS_PER_CLIENT:
        raise PartPlacementError(
            f"client part {client_part} needs {count} internal parts "
            f"but between 1 and {MAX_INTERNAL_PARTS_PER_CLIENT} are allowed"
        )

    below = above = 0
    widest = count
    for part, (placed_start, placed_end) in allocations.items():
        if part == client_part:
            continue
        widest = max(widest, placed_end - placed_start + 1)
        if part < client_part:
            below = max(below, part)
        elif not above or part < above:
            above = part
    has_others = len(allocations) > (client_part in allocations)
    low = allocations[below][1] + 1 if below else 1
    high = allocations[above][0] - 1 if above else S3_MAX_PART_NUMBER
    gap_below = client_part - below - 1
    gap_above = above - client_part - 1 if above else 0
    first = low + gap_below
    last = high - count + 1 - gap_above
    if first > last:
        raise PartPlacementError(
            f"client part {client_part} needs {count} internal part(s) but only "
            f"{high - low + 1 - gap_below - gap_above} are free between the parts already "
            "placed around it; upload parts of uniform size, or restart the upload"
        )

    previous = allocations.get(client_part)
    if previous is not None and first <= previous[0] <= last:
        return previous[0], previous[0] + count - 1

    if single_internal:
        ambiguous = not has_others and 1 < client_part <= AMBIGUOUS_FIRST_PART_MAX
        preferred = internal_part_range(client_part, 1)[0] if ambiguous else client_part
    else:
        preferred = internal_part_range(client_part, 1)[0]
    if first <= preferred <= last:
        start = preferred
    else:
        start = min(max(low + gap_below * widest, first), last)
    return start, start + count - 1
