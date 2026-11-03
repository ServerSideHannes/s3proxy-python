"""Client parts must get disjoint internal part ranges ordered like the client parts.

S3 assembles the object in internal-number order and reads locate client part N by
summing the sizes of lower client parts, so a violation either fails Complete with
InvalidPart (duplicate number) or yields an object that cannot be decrypted.
"""

import itertools
import random

import pytest

from s3proxy import crypto
from s3proxy.errors import S3Error
from s3proxy.handlers import S3ProxyHandler
from s3proxy.state import InternalPartMetadata, MultipartUploadState, PartMetadata
from s3proxy.state.allocation import (
    AMBIGUOUS_FIRST_PART_MAX,
    PartPlacementError,
    place_client_part,
)
from s3proxy.state.serialization import (
    deserialize_upload_state,
    json_dumps,
    json_loads,
    serialize_upload_state,
)

S3_MAX = crypto.S3_MAX_PART_NUMBER


def assert_ordered_and_disjoint(ranges: dict[int, tuple[int, int]]) -> None:
    previous_end = 0
    for part in sorted(ranges):
        start, end = ranges[part]
        assert previous_end < start <= end <= S3_MAX, f"part {part} {ranges[part]} in {ranges}"
        previous_end = end


def place_in_order(order, counts) -> dict[int, tuple[int, int]]:
    """Drive place_client_part the way the manager does, without the state store."""
    ranges: dict[int, tuple[int, int]] = {}
    single = True
    for part in order:
        if counts[part] > 1:
            single = False
        ranges[part] = place_client_part(ranges, part, counts[part], single_internal=single)
    return ranges


def uniform_counts(parts: int, full: int, last: int) -> dict[int, int]:
    return {p: full if p < parts else last for p in range(1, parts + 1)}


async def allocate_all(manager, upload_id, order, counts):
    await manager.create_upload("b", "k", upload_id, crypto.generate_dek())
    for part in order:
        await manager.allocate_internal_parts(
            "b", "k", upload_id, counts[part], client_part_number=part
        )
    return (await manager.get_upload("b", "k", upload_id)).allocations


class TestProductionFailure:
    async def test_short_last_part_arriving_before_a_two_internal_first_part(self, manager):
        """pg_basebackup: 7 MiB part 2 reached the proxy before 16 MiB part 1."""
        ranges = await allocate_all(manager, "u", order=[2, 1], counts={1: 2, 2: 1})

        assert_ordered_and_disjoint(ranges)
        assert ranges[1] == (1, 2)

    async def test_complete_lists_each_internal_number_once_in_client_order(
        self, manager, settings
    ):
        ranges = await allocate_all(manager, "u", order=[2, 1], counts={1: 2, 2: 1})
        state = await manager.get_upload("b", "k", "u")
        for part, (start, end) in ranges.items():
            internals = [InternalPartMetadata(n, 1, 29, f"e{n}") for n in range(start, end + 1)]
            state.parts[part] = PartMetadata(part, len(internals), 29 * len(internals), "e", "m")
            state.parts[part].internal_parts = internals

        handler = S3ProxyHandler(settings, {}, manager)
        s3_parts, _, _ = handler._build_s3_parts(
            [{"PartNumber": 1, "ETag": '"m"'}, {"PartNumber": 2, "ETag": '"m"'}],
            state,
            "b",
            "k",
            "u",
        )
        numbers = [p["PartNumber"] for p in s3_parts]
        assert numbers == sorted(set(numbers)) and len(numbers) == 3


class TestArrivalOrders:
    @pytest.mark.parametrize("full", [1, 2, 6, 20])
    def test_every_arrival_order_of_up_to_six_uniform_parts(self, full):
        """Any order works when parts are one size and only the last may be smaller."""
        for parts in range(1, 7):
            for last in range(1, full + 1):
                counts = uniform_counts(parts, full, last)
                for order in itertools.permutations(range(1, parts + 1)):
                    ranges = place_in_order(order, counts)
                    assert_ordered_and_disjoint(ranges)
                    assert {p: e - s + 1 for p, (s, e) in ranges.items()} == counts

    async def test_manager_places_every_arrival_order_of_three_parts(self, manager):
        for n, order in enumerate(itertools.permutations([1, 2, 3])):
            counts = {1: 2, 2: 2, 3: 1}
            ranges = await allocate_all(manager, f"u{n}", order, counts)
            assert_ordered_and_disjoint(ranges)

    @pytest.mark.parametrize("threads", [1, 4, 10, 32])
    @pytest.mark.parametrize("full", [1, 2, 6, 20])
    def test_threaded_client_arrival_orders(self, threads, full):
        """Parts start `threads` at a time and arrive in random order within each batch."""
        rng = random.Random(threads * 100 + full)
        for _ in range(200):
            parts = rng.randint(1, min(120, 9000 // full))
            counts = uniform_counts(parts, full, rng.randint(1, full))
            order = list(range(1, parts + 1))
            arrival = []
            for i in range(0, parts, threads):
                batch = order[i : i + threads]
                rng.shuffle(batch)
                arrival += batch
            ranges = place_in_order(arrival, counts)
            assert_ordered_and_disjoint(ranges)
            assert {p: e - s + 1 for p, (s, e) in ranges.items()} == counts

    @pytest.mark.slow
    def test_single_internal_parts_stay_dense_up_to_the_s3_limit(self):
        """ClickHouse-style uploads of thousands of 5 MiB parts, here arriving in reverse."""
        order = range(S3_MAX, 0, -1)
        ranges = place_in_order(order, dict.fromkeys(order, 1))
        assert all(r == (p, p) for p, r in ranges.items())

    @pytest.mark.slow
    def test_first_arrival_reservation_is_bounded(self):
        """A short last part arriving first reserves room below it, at a bounded cost."""

        def parts_placed(first: int) -> int:
            ranges: dict[int, tuple[int, int]] = {}
            order = [first, *(p for p in range(1, S3_MAX + 1) if p != first)]
            for placed, part in enumerate(order):
                try:
                    ranges[part] = place_client_part(ranges, part, 1, single_internal=True)
                except PartPlacementError:
                    return placed
            return len(order)

        assert parts_placed(AMBIGUOUS_FIRST_PART_MAX) >= S3_MAX - 19 * (
            AMBIGUOUS_FIRST_PART_MAX - 1
        )
        assert parts_placed(AMBIGUOUS_FIRST_PART_MAX + 1) == S3_MAX

    def test_sparse_layout_is_kept_when_safe(self):
        ranges = place_in_order([1, 2, 3], dict.fromkeys([1, 2, 3], 6))
        assert ranges == {1: (1, 6), 2: (21, 26), 3: (41, 46)}


class TestUnsafeLayoutsAreRejected:
    def test_never_returns_an_overlapping_or_misordered_placement(self):
        """Mixed part sizes in random order either place safely or raise, never overlap."""
        rng = random.Random(7)
        rejected = placed = 0
        for _ in range(2000):
            parts = rng.randint(2, 12)
            counts = {p: rng.choice([1, 1, 2, 3, 20]) for p in range(1, parts + 1)}
            order = rng.sample(range(1, parts + 1), parts)
            ranges: dict[int, tuple[int, int]] = {}
            single = True
            for part in order:
                single = single and counts[part] == 1
                try:
                    ranges[part] = place_client_part(
                        ranges, part, counts[part], single_internal=single
                    )
                except PartPlacementError:
                    rejected += 1
                    break
                assert_ordered_and_disjoint(ranges)
                placed += 1
        assert rejected and placed

    def test_rejection_message_says_what_to_do(self):
        ranges = {2: (2, 2)}
        with pytest.raises(PartPlacementError, match="uniform size"):
            place_client_part(ranges, 1, 2, single_internal=False)

    @pytest.mark.parametrize("part", [0, S3_MAX + 1])
    def test_part_number_bounds(self, part):
        with pytest.raises(PartPlacementError):
            place_client_part({}, part, 1, single_internal=True)

    @pytest.mark.parametrize("count", [0, crypto.MAX_INTERNAL_PARTS_PER_CLIENT + 1])
    def test_internal_part_count_bounds(self, count):
        with pytest.raises(PartPlacementError):
            place_client_part({}, 1, count, single_internal=False)

    async def test_manager_maps_rejection_to_invalid_part(self, manager):
        await manager.create_upload("b", "k", "u", crypto.generate_dek())
        await manager.allocate_internal_parts("b", "k", "u", 1, client_part_number=3)
        await manager.allocate_internal_parts("b", "k", "u", 1, client_part_number=2)
        with pytest.raises(S3Error) as exc:
            # Parts 2 and 3 were both single-internal, so only number 1 is left below them.
            await manager.allocate_internal_parts("b", "k", "u", 2, client_part_number=1)
        assert exc.value.code == "InvalidPart"
        assert (await manager.get_upload("b", "k", "u")).allocations.get(1) is None


class TestRetries:
    def test_re_uploaded_part_keeps_its_range(self):
        ranges = place_in_order([1, 2, 3], dict.fromkeys([1, 2, 3], 2))
        assert place_client_part(ranges, 2, 2, single_internal=False) == ranges[2]

    def test_re_uploaded_part_may_shrink_or_grow_within_its_window(self):
        ranges = place_in_order([1, 2, 3], dict.fromkeys([1, 2, 3], 6))
        assert place_client_part(ranges, 2, 3, single_internal=False) == (21, 23)
        assert place_client_part(ranges, 2, 20, single_internal=False) == (21, 40)


class TestPersistence:
    async def test_allocations_survive_the_store_round_trip(self, manager):
        await allocate_all(manager, "u", order=[3, 1], counts={1: 2, 3: 1})
        state = await manager.get_upload("b", "k", "u")
        assert deserialize_upload_state(serialize_upload_state(state)).allocations == (
            state.allocations
        )

    async def test_state_written_before_allocations_existed_protects_completed_parts(self, manager):
        """Redis blobs from an older pod hold parts but no allocation map."""
        await manager.create_upload("b", "k", "u", crypto.generate_dek())
        await manager.add_part(
            "b",
            "k",
            "u",
            PartMetadata(
                2, 10, 38, "e", "m", internal_parts=[InternalPartMetadata(2, 10, 38, "i")]
            ),
        )
        key = manager._storage_key("b", "k", "u")
        blob = json_loads(await manager._store.get(key))
        blob.pop("allocations", None)
        await manager._store.set(key, json_dumps(blob), manager._ttl)

        assert (await manager.get_upload("b", "k", "u")).allocations == {2: (2, 2)}
        with pytest.raises(S3Error) as exc:
            await manager.allocate_internal_parts("b", "k", "u", 2, client_part_number=1)
        assert exc.value.code == "InvalidPart"

    async def test_in_flight_part_is_visible_to_the_next_allocation(self, manager):
        """Allocation is recorded when the part starts, not when it finishes."""
        await manager.create_upload("b", "k", "u", crypto.generate_dek())
        a = await manager.allocate_internal_parts("b", "k", "u", 1, client_part_number=2)
        b = await manager.allocate_internal_parts("b", "k", "u", 2, client_part_number=1)
        assert b + 1 < a


class TestCompleteGuard:
    def _state(self, parts: dict[int, list[int]]) -> MultipartUploadState:
        state = MultipartUploadState(dek=b"k" * 32, bucket="b", key="k", upload_id="u")
        for part, numbers in parts.items():
            internals = [InternalPartMetadata(n, 1, 29, f"e{n}") for n in numbers]
            state.parts[part] = PartMetadata(part, len(numbers), 29 * len(numbers), "e", "m")
            state.parts[part].internal_parts = internals
        return state

    def _build(self, settings, manager, parts):
        handler = S3ProxyHandler(settings, {}, manager)
        listed = [{"PartNumber": p, "ETag": '"m"'} for p in sorted(parts)]
        return handler._build_s3_parts(listed, self._state(parts), "b", "k", "u")

    def test_duplicate_internal_number_is_rejected_before_calling_s3(self, settings, manager):
        with pytest.raises(S3Error) as exc:
            self._build(settings, manager, {1: [1, 2], 2: [2]})
        assert exc.value.code == "InvalidPart"

    def test_internal_numbers_out_of_client_order_are_rejected(self, settings, manager):
        with pytest.raises(S3Error) as exc:
            self._build(settings, manager, {1: [21, 22], 2: [3]})
        assert exc.value.code == "InvalidPart"

    def test_ordered_internal_numbers_pass(self, settings, manager):
        s3_parts, _, _ = self._build(settings, manager, {1: [1, 2], 2: [21], 3: [22]})
        assert [p["PartNumber"] for p in s3_parts] == [1, 2, 21, 22]
