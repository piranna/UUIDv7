#!/usr/bin/env python3
"""
uuid7.py

This module defines the UUIDv7 class, which extends the standard UUID
class to generate UUIDs based on a combination of timestamp, counter,
and random bits. The UUIDv7 class allows for customizable configurations
of timestamp precision, counter bits, and random bits, providing
flexibility for various use cases that require unique identifiers with
specific properties.

Classes:
    UUIDv7: A class for generating UUID version 7 identifiers.

Author:
    Jesús Leganés-Combarro 'piranna' (https://piranna.github.io)
    for TRC (https://trc.es/)

Copyright:
    (c) 2024 Jesús Leganés-Combarro. All rights reserved.

License:
    MIT
"""

from datetime import datetime, timezone
from math import floor
from random import SystemRandom
from time import time_ns
from uuid import UUID


NS_IN_MS = 1_000_000  # Nanoseconds in a millisecond


# Keep track of the last timestamp and counter for each bit length
_counters = {}

_random = SystemRandom()
_getrandbits = _random.getrandbits
_randrange = _random.randrange


def _calc_counter_and_random(
    unix_ts_ms_fraction_num_bits: int, counter_num_bits: int,
    monotonic_random: bool, counter: None | int,
    counter_guard_seed_num_bits: int, counter_step: int, random: None | int,
    unix_ts_ms_with_fraction: int, random_num_bits: int
):
    "Calculate the counter and random values"

    # Get the last timestamp and counters for the given timestamp fraction
    last_timestamp, counters = _counters.get(
        unix_ts_ms_fraction_num_bits, (0, {})
    )

    assert last_timestamp <= unix_ts_ms_with_fraction, (
        "Timestamps are not monotonic"
    )

    # Get the last counter and random values for the given timestamp
    # fraction and counter bit length
    if (
        last_timestamp < unix_ts_ms_with_fraction or
        counter_num_bits not in counters
    ):
        last_counter = last_random = None
    else:
        last_counter, last_random = counters[counter_num_bits]

    # Calculate the counter and random values
    if monotonic_random:  # Monotonic Random (Method 2)
        counter, random = _counter_method2(
            counter_num_bits, counter, counter_guard_seed_num_bits,
            counter_step, random, random_num_bits, last_counter, last_random
        )

    else:  # Fixed Bit-Length Dedicated Counter (Method 1)
        counter, random = _counter_method1(
            counter_num_bits, counter, counter_guard_seed_num_bits,
            counter_step, random, random_num_bits, last_counter
        )

    # Update the counters
    counters[counter_num_bits] = (counter, random)

    _counters[unix_ts_ms_fraction_num_bits] = (
        unix_ts_ms_with_fraction, counters
    )

    # Return the counter and random values
    return counter, random


def _compose_data(
    unix_ts_ms_with_fraction, unix_ts_ms_fraction_num_bits, counter,
    random_num_bits, random
):
    "Compose the data with the timestamp, counters and random data"
    # Compose data with timestamp, counters and random data. We are not
    # doing it directly since values can span over both `rand_a` and
    # `rand_b` fields in a non-fixed way, so we'll split them later.
    data = (
        (unix_ts_ms_with_fraction << (74 - unix_ts_ms_fraction_num_bits)) |
        (counter << random_num_bits) |
        random
    )

    # Split the random number into the `unix_ts_ms` and the two random
    # fields
    unix_ts_ms = data >> 74
    rand_a = data >> 62 & 0x0fff
    rand_b = data & ~(~0 << 62)

    # Construct the UUID7 integer
    return (
        (unix_ts_ms << 80) |
        (0b0111 << 76) |  # UUID version 7
        (rand_a << 64) |
        (0b10 << 62) |  # UUID variant (RFC 9562)
        rand_b
    )


def _counter_method1(
    counter_num_bits, counter, counter_guard_seed_num_bits, counter_step,
    random, random_num_bits, last_counter
) -> tuple[int, int]:
    "Fixed Bit-Length Dedicated Counter (Method 1)"

    if counter is None:
        counter = _increment_counter(
            counter_num_bits, counter_guard_seed_num_bits, counter_step,
            last_counter
        )

    elif last_counter is not None:
        assert last_counter < counter

    if random is None:
        random = _getrandbits(random_num_bits)

    return counter, random


def _counter_method2(
    counter_num_bits, counter, counter_guard_seed_num_bits, counter_step,
    random, random_num_bits, last_counter, last_random
) -> tuple[int, int]:
    "Monotonic Random (Method 2)"

    # Use `random` if provided and valid
    if random is not None:
        if last_random is None:
            return 0, random

        # `last_counter` is not None, too
        if counter is None:
            assert last_random < random, "Random is not monotonic"
            return last_counter, random

        if last_counter != counter:
            assert last_counter < counter
        else:
            assert last_random < random, "Random is not monotonic"

        return counter, random

    # Use `counter` if provided and valid
    if counter is not None:
        if last_counter is None:
            random = _getrandbits(random_num_bits)

        # `last_random` is not None, too
        else:
            assert last_counter <= counter

            random = (
                _getrandbits(random_num_bits)
                if last_counter < counter else
                _randrange(last_random, 1 << random_num_bits)
            )

        return counter, random

    random = _getrandbits(random_num_bits)

    # Seed the counter and random if timestamp has changed
    if last_random is None:  # `last_counter` is None, too
        return 0, random

    # Increment the random
    random = last_random + 1 + random

    if random < (1 << random_num_bits):
        return last_counter, random

    # Increment counter if random overflows (rollover)
    assert counter_num_bits, "Counter is required as guard for random overflow"

    return (
        _increment_counter(
            counter_num_bits, counter_guard_seed_num_bits, counter_step,
            last_counter
        ),
        random & ~(~0 << random_num_bits)  # Truncate the overflow random bits
    )


def _increment_counter(
    counter_num_bits, counter_guard_seed_num_bits, counter_step, last_counter
):
    # Calculate the counter
    if last_counter is None:
        return _init_counter(counter_num_bits, counter_guard_seed_num_bits)

    counter = last_counter + counter_step

    # Check the counter doesn't overflow (rollover)
    assert counter < (1 << counter_num_bits), "Counter overflow"

    return counter


def _init_counter(counter_num_bits: int, counter_guard_seed_num_bits: int):
    "Initialize the counter"
    return _getrandbits(counter_num_bits - counter_guard_seed_num_bits)


def _normalize_timestamp(timestamp: None | datetime | float | int) -> int:
    """Normalize the timestamp to milliseconds with 12 bits fraction

    The timestamp can be provided as:
    - `None`: use the current time
    - `datetime`: a datetime object
    - `float`: seconds since the epoch
    - `int`: nanoseconds since the epoch

    Return the timestamp in milliseconds as int with 48+12 bits fraction
    """

    # NOTE: For speed and prevent float rounding errors, calcs are done
    #       using bit-wise and integer operations as 48+12 bits

    if timestamp is None:
        timestamp = time_ns()  # nanoseconds as `int` since epoch
    elif isinstance(timestamp, datetime):
        timestamp = timestamp.timestamp()  # seconds as `float` since epoch

    if isinstance(timestamp, int):  # nanoseconds as `int` since epoch
        result = (timestamp << 12) // NS_IN_MS
    elif isinstance(timestamp, float):  # seconds as `float` since epoch
        result = floor(timestamp * 1000 * 2**12)
    else:
        raise TypeError("Invalid timestamp type")

    assert 0 <= result < (1 << 60)  # Ensure it fits in 48+12 bits (truncate?)

    return result


class UUIDv7(UUID):
    """
    UUIDv7 class for generating UUID version 7.

    Methods:
        __init__(self, unix_ts_ms_fraction_num_bits=0, counter_num_bits=0, monotonic_random=False, *, timestamp=None, counter=None, counter_guard_seed_num_bits=0, counter_step=1, counter_use_spec_recommended_num_bits=True, random=None):
            Initialize the UUIDv7 class with various parameters for timestamp, counter, and random values.

    Attributes:
        counter (int): The counter value used in the UUID.
        datetime (datetime): The datetime object representing the timestamp.
        random (int): The random value used in the UUID.

        fields(self): The fields of the UUID.

        unix_ts_ms(self): The Unix timestamp in milliseconds.
        rand_a(self): The first part of the random value.
        rand_b(self): The second part of the random value.
    """

    def __init__(
        self,

        # Timestamp
        unix_ts_ms_fraction_num_bits: int = 0,

        # Counter
        counter_num_bits: int = 0,

        # Random
        monotonic_random: bool = False,

        *,  # Keyword-only arguments

        # Timestamp
        timestamp: None | datetime | float | int = None,

        # Counter
        counter=None,
        counter_guard_seed_num_bits: int = 0,
        counter_step: int = 1,
        counter_use_spec_recommended_num_bits: bool = True,

        # Random
        random: None | int = None
    ):
        "Initialize the UUID7 class"

        # Validate the input
        assert 0 <= unix_ts_ms_fraction_num_bits <= 12, (
            "Invalid number of bits for the timestamp fraction"
        )

        if monotonic_random:
            if random is None:
                assert (
                    0 <= counter_num_bits <= 74 - unix_ts_ms_fraction_num_bits
                ), "Invalid number of bits for the counter (monotonic random guard)"

                assert timestamp is not None, (
                    "Random is required when timestamp is provided"
                )

        elif counter_num_bits:
            if counter_use_spec_recommended_num_bits:
                assert 12 <= counter_num_bits <= 42, (
                    "Invalid number of bits for the counter"
                )
            else:
                assert (
                    0 < counter_num_bits <= 74 - unix_ts_ms_fraction_num_bits
                ), "Invalid number of bits for the counter"

        if counter is None:
            assert 0 <= counter_guard_seed_num_bits <= counter_num_bits, (
                "Invalid number of bits for the counter guard seed"
            )

            if counter_num_bits:
                assert timestamp is not None, (
                    "Counter is required when timestamp is provided"
                )

                assert 0 < counter_step < (1 << counter_num_bits), (
                    "Invalid number of bits for the counter step"
                )
        else:
            assert counter_num_bits, "counter_num_bits is required"
            assert 0 <= counter < (1 << counter_num_bits), (
                "Invalid number of bits for the counter"
            )

        random_num_bits = 74 - unix_ts_ms_fraction_num_bits - counter_num_bits

        if random is not None:
            assert 0 <= random < (1 << random_num_bits), (
                "Invalid number of bits for the frozen random counter step"
            )

        # Timestamp in milliseconds (with 12 bits fraction)
        unix_ts_ms_with_12bits_fraction = _normalize_timestamp(timestamp)

        # OPTIONAL sub-milliseconds timestamp fraction
        # Replace Leftmost Random Bits with Increased Clock Precision
        # (Method 3)
        unix_ts_ms_with_fraction = (
            unix_ts_ms_with_12bits_fraction >> (
                12 - unix_ts_ms_fraction_num_bits
            )
        )

        # OPTIONAL carefully seeded counter
        if timestamp is None:
            counter, random = _calc_counter_and_random(
                unix_ts_ms_fraction_num_bits, counter_num_bits,
                monotonic_random, counter, counter_guard_seed_num_bits,
                counter_step, random, unix_ts_ms_with_fraction, random_num_bits
            )

        else:
            # Timestamp is provided, and we can't check the counter and
            # random values for monotonicity or against collisions, so
            # we'll use them as provided
            if counter is None:
                counter = _init_counter(
                    counter_num_bits, counter_guard_seed_num_bits
                )

            if random is None:
                random = _getrandbits(random_num_bits)

        # Generate the UUID
        super().__init__(
            int=_compose_data(
                unix_ts_ms_with_fraction, unix_ts_ms_fraction_num_bits,
                counter, random_num_bits, random
            )
        )

        # Expose the values
        dt = datetime.fromtimestamp(
            unix_ts_ms_with_fraction /
            (1000 * 2**unix_ts_ms_fraction_num_bits),
            tz=timezone.utc
        )

        # HACK: We need to set the attributes directly since the UUID
        #       class doesn't allow to set them
        object.__setattr__(self, 'counter', counter)
        object.__setattr__(self, 'datetime', dt)
        object.__setattr__(self, 'random', random)

    @ property
    def fields(self):
        return (self.unix_ts_ms, self.rand_a, self.rand_b)

    @ property
    def unix_ts_ms(self):
        return self.int >> 80

    @ property
    def rand_a(self):
        return self.int >> 64 & 0x0fff

    @ property
    def rand_b(self):
        return self.int & ~(~0 << 62)


uuid7 = UUIDv7
uuid7.__doc__ = UUIDv7.__doc__


if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser(description="Generate UUIDv7 identifiers")
    parser.add_argument(
        '--unix-ts-ms-fraction-num-bits', type=int, default=0,
        help="Number of bits for the timestamp fraction (default: 0)"
    )
    parser.add_argument(
        '--counter-num-bits', type=int, default=0,
        help="Number of bits for the counter (default: 0)"
    )
    parser.add_argument(
        '--monotonic-random', action='store_true',
        help="Use monotonic random (default: False)"
    )
    parser.add_argument(
        '-t', '--timestamp', type=int,
        help="Timestamp in milliseconds since the epoch (default: None)"
    )
    parser.add_argument(
        '-c', '--counter', type=int,
        help="Counter value (default: None)"
    )
    parser.add_argument(
        '--counter-guard-seed-num-bits', type=int, default=0,
        help="Number of bits for the counter guard seed (default: 0)"
    )
    parser.add_argument(
        '--counter-step', type=int, default=1,
        help="Counter step (default: 1)"
    )
    parser.add_argument(
        '--counter-use-spec-recommended-num-bits', action='store_true',
        help="Use the recommended number of bits for the counter (default: True)"
    )
    parser.add_argument(
        '-r', '--random', type=int,
        help="Random value (default: None)"
    )

    args = parser.parse_args()

    print(
        uuid7(
            args.unix_ts_ms_fraction_num_bits,
            args.counter_num_bits,
            args.monotonic_random,
            timestamp=args.timestamp,
            counter=args.counter,
            counter_guard_seed_num_bits=args.counter_guard_seed_num_bits,
            counter_step=args.counter_step,
            counter_use_spec_recommended_num_bits=(
                args.counter_use_spec_recommended_num_bits
            ),
            random=args.random
        )
    )
