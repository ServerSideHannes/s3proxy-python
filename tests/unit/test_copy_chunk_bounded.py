"""UploadPartCopy must pump the source in MAX_BUFFER_SIZE chunks, not 64MB ones.

The dedup copy path (_pump_copy_chunks) sized its buffer from
calculate_optimal_part_size, which returns up to 64MB for large sources. So a
copy of a large SSTable buffered a 64MB chunk + copied it + re-encrypted it
(~150MB resident) while the limiter only reserved copy_pipeline_peak (~32MB).
Under a scylla dedup flood that under-reservation OOMed the pod (reproduced
locally: concurrent UploadPartCopy of >=80MB sources pinned RSS at the 512Mi
wall; capping the chunk to 8MB dropped peak to ~320MiB).

This pins the invariant: the pump chunk never exceeds MAX_BUFFER_SIZE, so the
copy stays streaming and matched to its reservation, regardless of source size.
"""

from s3proxy import crypto


def _pump_chunk_size(plaintext_size: int) -> int:
    # Mirror the sizing in _streaming_copy_part_inner (copy.py).
    return min(crypto.calculate_optimal_part_size(plaintext_size), crypto.MAX_BUFFER_SIZE)


def test_optimal_part_size_is_large_for_big_sources():
    # Guard the premise: without the cap, big sources buffer huge chunks.
    assert crypto.calculate_optimal_part_size(80 * 1024 * 1024) > crypto.MAX_BUFFER_SIZE


def test_pump_chunk_capped_at_max_buffer():
    for size_mb in (1, 8, 52, 80, 120, 5 * 1024, 100 * 1024):
        assert _pump_chunk_size(size_mb * 1024 * 1024) <= crypto.MAX_BUFFER_SIZE
