#!/usr/bin/env python3

from datetime import datetime, timezone
from time import time
from unittest import TestCase, main
from uuid import UUID

from uuid7 import _counters, uuid7, UUIDv7


utc = timezone.utc


class TestUUIDv7(TestCase):
    def setUp(self) -> None:
        _counters.clear()

    def test_uuid(self):
        "Test the creation of a UUIDv7 instance"
        uuid_instance = uuid7()

        self.assertIsInstance(uuid_instance, UUID)
        self.assertIsInstance(uuid_instance, UUIDv7)
        self.assertEqual(uuid_instance.version, 7)
        self.assertEqual(uuid_instance.variant, 'specified in RFC 4122')

        self.assertIsInstance(uuid_instance.counter, int)
        self.assertIsInstance(uuid_instance.datetime, datetime)
        self.assertIsInstance(uuid_instance.random, int)
        self.assertEqual(uuid_instance.datetime.tzinfo, utc)

        # Test the string representation of a UUIDv7 instance
        uuid_str = str(uuid_instance)

        self.assertIsInstance(uuid_str, str)
        self.assertEqual(len(uuid_str), 36)  # UUID string length

        # Test the fields property of a UUIDv7 instance
        fields = uuid_instance.fields

        self.assertIsInstance(fields, tuple)
        self.assertEqual(len(fields), 3)  # unix_ts_ms, rand_a, rand_b

        # Test the unix_ts_ms property of a UUIDv7 instance
        unix_ts_ms = uuid_instance.unix_ts_ms

        self.assertIsInstance(unix_ts_ms, int)

        # Test the rand_a property of a UUIDv7 instance
        rand_a = uuid_instance.rand_a

        self.assertIsInstance(rand_a, int)

        # Test the rand_b property of a UUIDv7 instance
        rand_b = uuid_instance.rand_b

        self.assertIsInstance(rand_b, int)

    def test_uuid_timestamp_field(self):
        "Test the creation of a UUIDv7 instance with full fields"
        timestamp = datetime.now(tz=utc)

        uuid_instance = uuid7(timestamp=timestamp)

        timestamp = timestamp.replace(  # Truncate microseconds to milliseconds
            microsecond=timestamp.microsecond // 1000 * 1000
        )

        self.assertEqual(uuid_instance.datetime, timestamp)
        self.assertEqual(
            datetime.fromtimestamp(uuid_instance.unix_ts_ms / 1000, tz=utc),
            timestamp
        )

        with self.assertRaises(TypeError):
            uuid7(timestamp='outatime')

    def test_uuid_create_from_fields(self):
        "Test the creation of a UUIDv7 instance with full fields"
        counter_num_bits = 12
        timestamp = time()
        counter = 42
        random = 0

        uuid_instance = uuid7(
            timestamp=timestamp, counter=counter,
            counter_num_bits=counter_num_bits, random=random
        )

        timestamp = datetime.fromtimestamp(timestamp, tz=utc)
        timestamp = timestamp.replace(  # Truncate microseconds to milliseconds
            microsecond=timestamp.microsecond // 1000 * 1000
        )

        self.assertEqual(uuid_instance.counter, counter)
        self.assertEqual(uuid_instance.datetime, timestamp)
        self.assertEqual(uuid_instance.random, random)
        self.assertEqual(
            datetime.fromtimestamp(uuid_instance.unix_ts_ms / 1000, tz=utc),
            timestamp
        )

    def test_uuid_counter_num_bits(self):
        "Test the creation of a UUIDv7 instance with a counter field"
        uuid_instance1 = uuid7(counter_num_bits=12)

        self.assertIsInstance(uuid_instance1, UUIDv7)

        uuid_instance2 = uuid7(counter_num_bits=12)

        self.assertEqual(uuid_instance1.unix_ts_ms, uuid_instance2.unix_ts_ms)
        self.assertLessEqual(uuid_instance1.counter, uuid_instance2.counter)

        uuid_instance3 = uuid7(
            counter=uuid_instance2.counter+1, counter_num_bits=12
        )

        self.assertEqual(uuid_instance2.unix_ts_ms, uuid_instance3.unix_ts_ms)
        self.assertLessEqual(uuid_instance2.counter, uuid_instance3.counter)

    def test_uuid_counter_field(self):
        "Test the creation of a UUIDv7 instance with a counter field"
        counter_num_bits = 6
        counter = 42
        counter_use_spec_recommended_num_bits = False

        uuid_instance = uuid7(
            counter=counter, counter_num_bits=counter_num_bits,
            counter_use_spec_recommended_num_bits=counter_use_spec_recommended_num_bits
        )

        self.assertEqual(uuid_instance.counter, counter)

    def test_uuid_monotonic_random(self):
        "Test the creation of a UUIDv7 instance with a monotonic random field"
        uuid_instance1 = uuid7(monotonic_random=True)

        self.assertIsInstance(uuid_instance1, UUIDv7)

        uuid_instance2 = uuid7(monotonic_random=True)

        self.assertEqual(uuid_instance1.unix_ts_ms, uuid_instance2.unix_ts_ms)
        self.assertEqual(uuid_instance1.counter, uuid_instance2.counter)
        self.assertLessEqual(uuid_instance1.random, uuid_instance2.random)

    def test_uuid_monotonic_random_explicit(self):
        "Test the creation of a UUIDv7 instance with a monotonic random field"
        uuid_instance1 = uuid7(monotonic_random=True)

        self.assertIsInstance(uuid_instance1, UUIDv7)

        uuid_instance2 = uuid7(
            monotonic_random=True, random=uuid_instance1.random+1
        )

        self.assertEqual(uuid_instance1.unix_ts_ms, uuid_instance2.unix_ts_ms)
        self.assertEqual(uuid_instance1.counter, uuid_instance2.counter)
        self.assertLessEqual(uuid_instance1.random, uuid_instance2.random)

    def test_uuid_monotonic_random_explicit_counter(self):
        "Test the creation of a UUIDv7 instance with a monotonic random field"
        uuid_instance1 = uuid7(counter_num_bits=12, monotonic_random=True)

        self.assertIsInstance(uuid_instance1, UUIDv7)

        uuid_instance2 = uuid7(
            counter=uuid_instance1.counter, counter_num_bits=12,
            monotonic_random=True, random=uuid_instance1.random+1
        )

        self.assertEqual(uuid_instance1.unix_ts_ms, uuid_instance2.unix_ts_ms)
        self.assertEqual(uuid_instance1.counter, uuid_instance2.counter)
        self.assertLessEqual(uuid_instance1.random, uuid_instance2.random)

    def test_uuid_monotonic_random_explicit_greater_counter(self):
        "Test the creation of a UUIDv7 instance with a monotonic random field"
        uuid_instance1 = uuid7(counter_num_bits=12, monotonic_random=True)

        self.assertIsInstance(uuid_instance1, UUIDv7)

        uuid_instance2 = uuid7(
            counter=uuid_instance1.counter+1, counter_num_bits=12,
            monotonic_random=True, random=uuid_instance1.random+1
        )

        self.assertEqual(uuid_instance1.unix_ts_ms, uuid_instance2.unix_ts_ms)
        self.assertLessEqual(uuid_instance1.counter, uuid_instance2.counter)

    def test_uuid_monotonic_random_rollover(self):
        "Test the creation of a UUIDv7 instance with a monotonic random field"
        uuid_instance1 = uuid7(
            monotonic_random=True, counter_num_bits=74,
            counter_use_spec_recommended_num_bits=False
        )

        self.assertIsInstance(uuid_instance1, UUIDv7)

        uuid_instance2 = uuid7(
            monotonic_random=True, counter_num_bits=74,
            counter_use_spec_recommended_num_bits=False
        )

        self.assertEqual(uuid_instance1.unix_ts_ms, uuid_instance2.unix_ts_ms)
        self.assertLessEqual(uuid_instance1.counter, uuid_instance2.counter)

    def test_uuid_counter_step_frozen_monotonic_random(self):
        "Test the creation of a UUIDv7 instance with a frozen random field"
        random = 0

        uuid_instance = uuid7(monotonic_random=True, random=random)

        self.assertEqual(uuid_instance.random, random)

    def test_uuid_counter_step_frozen_monotonic_counter(self):
        "Test the creation of a UUIDv7 instance with a counter field"
        counter = 0

        uuid_instance1 = uuid7(
            monotonic_random=True, counter=counter, counter_num_bits=42
        )

        self.assertEqual(uuid_instance1.counter, counter)

        uuid_instance2 = uuid7(
            monotonic_random=True, counter=counter, counter_num_bits=42
        )

        self.assertEqual(uuid_instance2.counter, counter)

    def test_uuid_counter_step_frozen_random(self):
        "Test the creation of a UUIDv7 instance with a frozen random field"
        random = 0

        uuid_instance = uuid7(random=random)

        self.assertEqual(uuid_instance.random, random)


if __name__ == '__main__':
    main()
