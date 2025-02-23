#!venv/bin/python3
import unittest

from pagerduty_service_uptime import (
    Alert,
    Filter,
    alerts_overlap,
    filter_alerts,
    intervals_gen,
    merge_overlapping_alerts,
    merge_two_alerts,
    parse_date,
    parse_time_delta,
)


class TestAlertsOverlap(unittest.TestCase):
    def test_not_overlapping_one_second(self) -> None:
        self.assertFalse(
            alerts_overlap(
                Alert([], parse_date("2020-10-01 10:00:00"), parse_date("2020-10-01 11:00:00")),
                Alert([], parse_date("2020-10-01 11:00:01"), parse_date("2020-10-01 12:00:00")),
            )
        )

    def test_not_overlapping_one_hour(self) -> None:
        self.assertFalse(
            alerts_overlap(
                Alert([], parse_date("2020-10-01 10:00:00"), parse_date("2020-10-01 11:00:00")),
                Alert([], parse_date("2020-10-01 12:00:00"), parse_date("2020-10-01 12:00:01")),
            )
        )

    def test_not_overlapping_one_day(self) -> None:
        self.assertFalse(
            alerts_overlap(
                Alert([], parse_date("2020-10-01 10:00:00"), parse_date("2020-10-01 11:00:00")),
                Alert([], parse_date("2020-10-02 10:00:00"), parse_date("2020-10-02 11:00:00")),
            )
        )

    def test_overlapping_started_same_second(self) -> None:
        self.assertTrue(
            alerts_overlap(
                Alert([], parse_date("2020-10-01 10:00:00"), parse_date("2020-10-01 11:00:00")),
                Alert([], parse_date("2020-10-01 10:00:00"), parse_date("2020-10-01 12:00:00")),
            )
        )

    def test_overlapping_started_one_second(self) -> None:
        self.assertTrue(
            alerts_overlap(
                Alert([], parse_date("2020-10-01 10:00:00"), parse_date("2020-10-01 12:00:00")),
                Alert([], parse_date("2020-10-01 10:00:01"), parse_date("2020-10-01 11:00:05")),
            )
        )

    def test_overlapping_same_second(self) -> None:
        self.assertTrue(
            alerts_overlap(
                Alert([], parse_date("2020-10-01 10:00:00"), parse_date("2020-10-01 11:00:00")),
                Alert([], parse_date("2020-10-01 11:00:00"), parse_date("2020-10-01 12:00:00")),
            )
        )

    def test_overlapping_one_second(self) -> None:
        self.assertTrue(
            alerts_overlap(
                Alert([], parse_date("2020-10-01 10:00:00"), parse_date("2020-10-01 11:00:01")),
                Alert([], parse_date("2020-10-01 11:00:00"), parse_date("2020-10-01 12:00:00")),
            )
        )

    def test_overlapping_one_hour(self) -> None:
        self.assertTrue(
            alerts_overlap(
                Alert([], parse_date("2020-10-01 10:00:00"), parse_date("2020-10-01 12:00:00")),
                Alert([], parse_date("2020-10-01 11:00:00"), parse_date("2020-10-01 12:05:00")),
            )
        )

    def test_overlapping_second_alert_ends_before_first(self) -> None:
        self.assertTrue(
            alerts_overlap(
                Alert([], parse_date("2020-10-01 10:00:00"), parse_date("2020-10-01 12:00:00")),
                Alert([], parse_date("2020-10-01 11:00:00"), parse_date("2020-10-01 11:05:00")),
            )
        )


class TestMergeTwoAlerts(unittest.TestCase):
    def test_overlapping_started_same_second(self) -> None:
        self.assertEqual(
            merge_two_alerts(
                Alert([10, 13], parse_date("2020-10-01 10:00:00"), parse_date("2020-10-01 12:00:00")),
                Alert([8], parse_date("2020-10-01 10:00:00"), parse_date("2020-10-01 11:00:00")),
            ),
            Alert([10, 13, 8], parse_date("2020-10-01 10:00:00"), parse_date("2020-10-01 12:00:00")),
        )

    def test_overlapping_same_second(self) -> None:
        self.assertEqual(
            merge_two_alerts(
                Alert([10, 13], parse_date("2020-10-01 10:00:00"), parse_date("2020-10-01 11:00:00")),
                Alert([8], parse_date("2020-10-01 11:00:00"), parse_date("2020-10-01 12:00:00")),
            ),
            Alert([10, 13, 8], parse_date("2020-10-01 10:00:00"), parse_date("2020-10-01 12:00:00")),
        )

    def test_overlapping_one_second(self) -> None:
        self.assertEqual(
            merge_two_alerts(
                Alert([9], parse_date("2020-10-01 10:00:00"), parse_date("2020-10-01 11:00:01")),
                Alert([1, 3], parse_date("2020-10-01 11:00:00"), parse_date("2020-10-01 12:00:00")),
            ),
            Alert([9, 1, 3], parse_date("2020-10-01 10:00:00"), parse_date("2020-10-01 12:00:00")),
        )

    def test_overlapping_one_hour(self) -> None:
        self.assertEqual(
            merge_two_alerts(
                Alert([1], parse_date("2020-10-01 10:00:00"), parse_date("2020-10-01 12:00:00")),
                Alert([2], parse_date("2020-10-01 11:00:00"), parse_date("2020-10-01 12:05:00")),
            ),
            Alert([1, 2], parse_date("2020-10-01 10:00:00"), parse_date("2020-10-01 12:05:00")),
        )

    def test_overlapping_second_alert_ends_before_first(self) -> None:
        self.assertEqual(
            merge_two_alerts(
                Alert([1, 3, 4], parse_date("2020-10-01 10:00:00"), parse_date("2020-10-01 12:00:00")),
                Alert([2, 5, 6], parse_date("2020-10-01 11:00:00"), parse_date("2020-10-01 11:05:00")),
            ),
            Alert([1, 3, 4, 2, 5, 6], parse_date("2020-10-01 10:00:00"), parse_date("2020-10-01 12:00:00")),
        )


class TestIntervalsGen(unittest.TestCase):
    def test1(self) -> None:
        intervals = list(
            intervals_gen(
                parse_date("2019-01-01 00:00:00"), parse_date("2020-01-01 00:00:00"), parse_time_delta("6 months")
            )
        )
        self.assertListEqual(
            intervals,
            [
                (parse_date("2019-01-01 00:00:00"), parse_date("2019-07-01 00:00:00")),
                (parse_date("2019-07-01 00:00:00"), parse_date("2020-01-01 00:00:00")),
            ],
        )

    def test2(self) -> None:
        intervals = list(
            intervals_gen(
                parse_date("2019-01-01 00:00:00"), parse_date("2019-12-31 23:59:59"), parse_time_delta("6 months")
            )
        )
        self.assertListEqual(
            intervals,
            [
                (parse_date("2019-01-01 00:00:00"), parse_date("2019-07-01 00:00:00")),
                (parse_date("2019-07-01 00:00:00"), parse_date("2019-12-31 23:59:59")),
            ],
        )

    def test3(self) -> None:
        intervals = list(
            intervals_gen(
                parse_date("2018-01-01 00:00:00"), parse_date("2020-01-01 00:00:00"), parse_time_delta("1 year")
            )
        )
        self.assertListEqual(
            intervals,
            [
                (parse_date("2018-01-01 00:00:00"), parse_date("2019-01-01 00:00:00")),
                (parse_date("2019-01-01 00:00:00"), parse_date("2020-01-01 00:00:00")),
            ],
        )

    def test4(self) -> None:
        intervals = list(
            intervals_gen(
                parse_date("2018-01-01 00:00:00"), parse_date("2019-01-01 00:00:00"), parse_time_delta("1 month")
            )
        )
        self.assertListEqual(
            intervals,
            [
                (parse_date("2018-01-01 00:00:00"), parse_date("2018-02-01 00:00:00")),
                (parse_date("2018-02-01 00:00:00"), parse_date("2018-03-01 00:00:00")),
                (parse_date("2018-03-01 00:00:00"), parse_date("2018-04-01 00:00:00")),
                (parse_date("2018-04-01 00:00:00"), parse_date("2018-05-01 00:00:00")),
                (parse_date("2018-05-01 00:00:00"), parse_date("2018-06-01 00:00:00")),
                (parse_date("2018-06-01 00:00:00"), parse_date("2018-07-01 00:00:00")),
                (parse_date("2018-07-01 00:00:00"), parse_date("2018-08-01 00:00:00")),
                (parse_date("2018-08-01 00:00:00"), parse_date("2018-09-01 00:00:00")),
                (parse_date("2018-09-01 00:00:00"), parse_date("2018-10-01 00:00:00")),
                (parse_date("2018-10-01 00:00:00"), parse_date("2018-11-01 00:00:00")),
                (parse_date("2018-11-01 00:00:00"), parse_date("2018-12-01 00:00:00")),
                (parse_date("2018-12-01 00:00:00"), parse_date("2019-01-01 00:00:00")),
            ],
        )

    def test5(self) -> None:
        intervals = list(
            intervals_gen(
                parse_date("2018-01-01 10:00:05"), parse_date("2019-01-01 00:00:00"), parse_time_delta("1 month")
            )
        )
        self.assertListEqual(
            intervals,
            [
                (parse_date("2018-01-01 10:00:05"), parse_date("2018-02-01 10:00:05")),
                (parse_date("2018-02-01 10:00:05"), parse_date("2018-03-01 10:00:05")),
                (parse_date("2018-03-01 10:00:05"), parse_date("2018-04-01 10:00:05")),
                (parse_date("2018-04-01 10:00:05"), parse_date("2018-05-01 10:00:05")),
                (parse_date("2018-05-01 10:00:05"), parse_date("2018-06-01 10:00:05")),
                (parse_date("2018-06-01 10:00:05"), parse_date("2018-07-01 10:00:05")),
                (parse_date("2018-07-01 10:00:05"), parse_date("2018-08-01 10:00:05")),
                (parse_date("2018-08-01 10:00:05"), parse_date("2018-09-01 10:00:05")),
                (parse_date("2018-09-01 10:00:05"), parse_date("2018-10-01 10:00:05")),
                (parse_date("2018-10-01 10:00:05"), parse_date("2018-11-01 10:00:05")),
                (parse_date("2018-11-01 10:00:05"), parse_date("2018-12-01 10:00:05")),
                (parse_date("2018-12-01 10:00:05"), parse_date("2019-01-01 00:00:00")),
            ],
        )

    def test6(self) -> None:
        intervals = list(
            intervals_gen(
                parse_date("2019-01-01 00:00:00"), parse_date("2020-01-01 00:00:00"), parse_time_delta("15 months")
            )
        )
        self.assertListEqual(
            intervals,
            [
                (parse_date("2019-01-01 00:00:00"), parse_date("2020-01-01 00:00:00")),
            ],
        )

    def test7(self) -> None:
        intervals = list(
            intervals_gen(
                parse_date("2025-01-29 00:00:00"), parse_date("2026-01-29 00:00:00"), parse_time_delta("1 month")
            )
        )
        self.assertListEqual(
            intervals,
            [
                # 2025-01-29 is a date in the middle of the month.
                # Keep the 29th day of the month for the remaining dates.
                # For February, use 2025-02-28, as that month does not have a 29th day.
                (parse_date("2025-01-29 00:00:00"), parse_date("2025-02-28 00:00:00")),
                (parse_date("2025-02-28 00:00:00"), parse_date("2025-03-29 00:00:00")),
                (parse_date("2025-03-29 00:00:00"), parse_date("2025-04-29 00:00:00")),
                (parse_date("2025-04-29 00:00:00"), parse_date("2025-05-29 00:00:00")),
                (parse_date("2025-05-29 00:00:00"), parse_date("2025-06-29 00:00:00")),
                (parse_date("2025-06-29 00:00:00"), parse_date("2025-07-29 00:00:00")),
                (parse_date("2025-07-29 00:00:00"), parse_date("2025-08-29 00:00:00")),
                (parse_date("2025-08-29 00:00:00"), parse_date("2025-09-29 00:00:00")),
                (parse_date("2025-09-29 00:00:00"), parse_date("2025-10-29 00:00:00")),
                (parse_date("2025-10-29 00:00:00"), parse_date("2025-11-29 00:00:00")),
                (parse_date("2025-11-29 00:00:00"), parse_date("2025-12-29 00:00:00")),
                (parse_date("2025-12-29 00:00:00"), parse_date("2026-01-29 00:00:00")),
            ],
        )

    def test8(self) -> None:
        intervals = list(
            intervals_gen(
                parse_date("2025-02-28 00:00:00"), parse_date("2026-01-31 00:00:00"), parse_time_delta("1 month")
            )
        )
        self.assertListEqual(
            intervals,
            # 2025-02-28 is the last day of the month.
            # Use the "last day of the month" rule for the remaining dates.
            [
                (parse_date("2025-02-28 00:00:00"), parse_date("2025-03-31 00:00:00")),
                (parse_date("2025-03-31 00:00:00"), parse_date("2025-04-30 00:00:00")),
                (parse_date("2025-04-30 00:00:00"), parse_date("2025-05-31 00:00:00")),
                (parse_date("2025-05-31 00:00:00"), parse_date("2025-06-30 00:00:00")),
                (parse_date("2025-06-30 00:00:00"), parse_date("2025-07-31 00:00:00")),
                (parse_date("2025-07-31 00:00:00"), parse_date("2025-08-31 00:00:00")),
                (parse_date("2025-08-31 00:00:00"), parse_date("2025-09-30 00:00:00")),
                (parse_date("2025-09-30 00:00:00"), parse_date("2025-10-31 00:00:00")),
                (parse_date("2025-10-31 00:00:00"), parse_date("2025-11-30 00:00:00")),
                (parse_date("2025-11-30 00:00:00"), parse_date("2025-12-31 00:00:00")),
                (parse_date("2025-12-31 00:00:00"), parse_date("2026-01-31 00:00:00")),
            ],
        )


class TestMergeOverlappingAlerts(unittest.TestCase):
    def test_some_alerts_overlaps(self) -> None:
        merged_alerts = merge_overlapping_alerts(
            [
                Alert([1], parse_date("2020-10-01 10:00:00"), parse_date("2020-10-01 11:00:00")),
                Alert([2], parse_date("2020-10-01 11:00:00"), parse_date("2020-10-01 11:00:01")),
                Alert([3], parse_date("2020-10-01 11:00:01"), parse_date("2020-10-01 11:00:02")),
                Alert([4], parse_date("2020-10-01 12:00:00"), parse_date("2020-10-01 12:00:05")),
                Alert([5], parse_date("2020-10-01 12:00:06"), parse_date("2020-10-01 13:00:00")),
                Alert([6], parse_date("2020-10-01 12:00:07"), parse_date("2020-10-01 12:30:00")),
                Alert([7], parse_date("2020-10-01 13:00:00"), parse_date("2020-10-01 13:00:00")),
                Alert([8], parse_date("2020-10-01 14:00:00"), parse_date("2020-10-01 14:00:01")),
                Alert([9], parse_date("2020-10-01 14:00:00"), parse_date("2020-10-01 14:10:10")),
                Alert([10], parse_date("2020-10-01 14:00:00"), parse_date("2020-10-01 14:05:00")),
                Alert([11], parse_date("2020-10-01 15:00:00"), parse_date("2020-10-01 15:05:00")),
                Alert([12], parse_date("2020-10-01 15:01:00"), parse_date("2020-10-01 15:01:05")),
                Alert([13], parse_date("2020-10-01 15:01:00"), parse_date("2020-10-01 15:01:06")),
            ]
        )
        self.assertListEqual(
            merged_alerts,
            [
                Alert([1, 2, 3], parse_date("2020-10-01 10:00:00"), parse_date("2020-10-01 11:00:02")),
                Alert([4], parse_date("2020-10-01 12:00:00"), parse_date("2020-10-01 12:00:05")),
                Alert([5, 6, 7], parse_date("2020-10-01 12:00:06"), parse_date("2020-10-01 13:00:00")),
                Alert([8, 9, 10], parse_date("2020-10-01 14:00:00"), parse_date("2020-10-01 14:10:10")),
                Alert([11, 12, 13], parse_date("2020-10-01 15:00:00"), parse_date("2020-10-01 15:05:00")),
            ],
        )

    def test_all_alerts_overlaps(self) -> None:
        merged_alerts = merge_overlapping_alerts(
            [
                Alert([1, 2], parse_date("2020-10-01 14:00:00"), parse_date("2020-10-01 14:00:05")),
                Alert([3, 4, 5], parse_date("2020-10-01 14:00:05"), parse_date("2020-10-01 14:10:10")),
                Alert([6], parse_date("2020-10-01 14:07:00"), parse_date("2020-10-01 14:11:00")),
            ]
        )
        self.assertListEqual(
            merged_alerts,
            [Alert([1, 2, 3, 4, 5, 6], parse_date("2020-10-01 14:00:00"), parse_date("2020-10-01 14:11:00"))],
        )

    def test_no_alerts_overlaps(self) -> None:
        merged_alerts = merge_overlapping_alerts(
            [
                Alert([1, 2], parse_date("2020-10-01 14:00:00"), parse_date("2020-10-01 14:00:01")),
                Alert([3, 4, 5], parse_date("2020-10-01 14:00:02"), parse_date("2020-10-01 14:10:10")),
                Alert([6], parse_date("2020-10-01 14:15:00"), parse_date("2020-10-01 14:15:05")),
            ]
        )
        self.assertListEqual(
            merged_alerts,
            [
                Alert([1, 2], parse_date("2020-10-01 14:00:00"), parse_date("2020-10-01 14:00:01")),
                Alert([3, 4, 5], parse_date("2020-10-01 14:00:02"), parse_date("2020-10-01 14:10:10")),
                Alert([6], parse_date("2020-10-01 14:15:00"), parse_date("2020-10-01 14:15:05")),
            ],
        )


class TestFilterAlerts(unittest.TestCase):
    def test_some_alert_matches_in_the_middle(self) -> None:
        filtered_alerts = filter_alerts(
            parse_date("2020-01-01 00:00:05"),
            parse_date("2020-03-03 10:00:05"),
            [
                Alert([1, 2], parse_date("2019-01-12 10:00:00"), parse_date("2019-01-13 11:00:02")),
                Alert([3], parse_date("2020-01-01 00:00:04"), parse_date("2020-02-01 00:00:00")),
                Alert([4, 5], parse_date("2020-01-01 00:00:05"), parse_date("2020-02-01 00:00:00")),
                Alert([6], parse_date("2020-01-01 00:00:06"), parse_date("2020-02-01 00:00:00")),
                Alert([7], parse_date("2020-02-01 12:00:00"), parse_date("2020-02-01 12:00:05")),
                Alert([8, 9], parse_date("2020-02-14 17:05:00"), parse_date("2020-02-14 17:00:35")),
                Alert([10], parse_date("2020-03-03 10:00:04"), parse_date("2020-03-03 17:00:35")),
                Alert([11, 12, 13], parse_date("2020-03-03 10:00:05"), parse_date("2020-03-03 17:00:35")),
                Alert([14, 15], parse_date("2020-03-03 10:00:06"), parse_date("2020-03-03 17:00:35")),
                Alert([16], parse_date("2020-04-01 12:00:00"), parse_date("2020-10-01 12:00:05")),
                Alert([17], parse_date("2020-07-01 12:00:00"), parse_date("2020-10-01 12:00:05")),
                Alert([18], parse_date("2021-01-01 12:00:00"), parse_date("2021-01-01 12:00:05")),
            ],
        )
        self.assertListEqual(
            filtered_alerts,
            [
                Alert([4, 5], parse_date("2020-01-01 00:00:05"), parse_date("2020-02-01 00:00:00")),
                Alert([6], parse_date("2020-01-01 00:00:06"), parse_date("2020-02-01 00:00:00")),
                Alert([7], parse_date("2020-02-01 12:00:00"), parse_date("2020-02-01 12:00:05")),
                Alert([8, 9], parse_date("2020-02-14 17:05:00"), parse_date("2020-02-14 17:00:35")),
                Alert([10], parse_date("2020-03-03 10:00:04"), parse_date("2020-03-03 17:00:35")),
            ],
        )

    def test_some_alert_matches_from_the_beginning_to_the_middle(self) -> None:
        filtered_alerts = filter_alerts(
            parse_date("2020-01-01 00:00:05"),
            parse_date("2020-03-03 10:00:05"),
            [
                Alert([4, 5], parse_date("2020-01-01 00:00:05"), parse_date("2020-02-01 00:00:00")),
                Alert([6], parse_date("2020-01-01 00:00:06"), parse_date("2020-02-01 00:00:00")),
                Alert([7], parse_date("2020-02-01 12:00:00"), parse_date("2020-02-01 12:00:05")),
                Alert([8, 9], parse_date("2020-02-14 17:05:00"), parse_date("2020-02-14 17:00:35")),
                Alert([10], parse_date("2020-03-03 16:00:04"), parse_date("2020-03-03 17:00:35")),
            ],
        )
        self.assertListEqual(
            filtered_alerts,
            [
                Alert([4, 5], parse_date("2020-01-01 00:00:05"), parse_date("2020-02-01 00:00:00")),
                Alert([6], parse_date("2020-01-01 00:00:06"), parse_date("2020-02-01 00:00:00")),
                Alert([7], parse_date("2020-02-01 12:00:00"), parse_date("2020-02-01 12:00:05")),
                Alert([8, 9], parse_date("2020-02-14 17:05:00"), parse_date("2020-02-14 17:00:35")),
            ],
        )

    def test_some_alert_matches_from_the_middle_to_the_end(self) -> None:
        filtered_alerts = filter_alerts(
            parse_date("2020-01-01 00:00:05"),
            parse_date("2020-03-03 10:00:05"),
            [
                Alert([1, 2], parse_date("2019-01-12 10:00:00"), parse_date("2019-01-13 11:00:02")),
                Alert([3], parse_date("2020-01-01 00:00:04"), parse_date("2020-02-01 00:00:00")),
                Alert([4, 5], parse_date("2020-01-01 00:00:05"), parse_date("2020-02-01 00:00:00")),
                Alert([6], parse_date("2020-01-01 00:00:06"), parse_date("2020-02-01 00:00:00")),
                Alert([7], parse_date("2020-02-01 12:00:00"), parse_date("2020-02-01 12:00:05")),
                Alert([8, 9], parse_date("2020-02-14 17:05:00"), parse_date("2020-02-14 17:00:35")),
                Alert([11], parse_date("2020-03-03 10:00:04"), parse_date("2020-03-03 17:00:35")),
            ],
        )
        self.assertListEqual(
            filtered_alerts,
            [
                Alert([4, 5], parse_date("2020-01-01 00:00:05"), parse_date("2020-02-01 00:00:00")),
                Alert([6], parse_date("2020-01-01 00:00:06"), parse_date("2020-02-01 00:00:00")),
                Alert([7], parse_date("2020-02-01 12:00:00"), parse_date("2020-02-01 12:00:05")),
                Alert([8, 9], parse_date("2020-02-14 17:05:00"), parse_date("2020-02-14 17:00:35")),
                Alert([11], parse_date("2020-03-03 10:00:04"), parse_date("2020-03-03 17:00:35")),
            ],
        )

    def test_all_alerts_matches(self) -> None:
        filtered_alerts = filter_alerts(
            parse_date("2020-01-01 00:00:05"),
            parse_date("2020-03-03 10:00:05"),
            [
                Alert([6], parse_date("2020-01-01 00:00:06"), parse_date("2020-02-01 00:00:00")),
                Alert([7], parse_date("2020-02-01 12:00:00"), parse_date("2020-02-01 12:00:05")),
                Alert([8, 9], parse_date("2020-02-14 17:05:00"), parse_date("2020-02-14 17:00:35")),
            ],
        )
        self.assertListEqual(
            filtered_alerts,
            [
                Alert([6], parse_date("2020-01-01 00:00:06"), parse_date("2020-02-01 00:00:00")),
                Alert([7], parse_date("2020-02-01 12:00:00"), parse_date("2020-02-01 12:00:05")),
                Alert([8, 9], parse_date("2020-02-14 17:05:00"), parse_date("2020-02-14 17:00:35")),
            ],
        )

    def test_no_alert_matches(self) -> None:
        filtered_alerts = filter_alerts(
            parse_date("2020-03-03 10:00:05"),
            parse_date("2020-03-03 10:00:07"),
            [
                Alert([6], parse_date("2020-01-01 00:00:06"), parse_date("2020-02-01 00:00:00")),
                Alert([7], parse_date("2020-02-01 12:00:00"), parse_date("2020-02-01 12:00:05")),
                Alert([8, 9], parse_date("2020-02-14 17:05:00"), parse_date("2020-02-14 17:00:35")),
            ],
        )
        self.assertListEqual(filtered_alerts, [])


class TestFilterClass(unittest.TestCase):
    def test_parse(self) -> None:
        cases = [
            (
                "priority.summary:matches:P1,P2",
                Filter(negation=False, path=["priority", "summary"], operator="matches", values=["P1", "P2"]),
            ),
            (
                "some.value.1:matches:P1|P2",
                Filter(negation=False, path=["some", "value", "1"], operator="matches", values=["P1|P2"]),
            ),
            (
                'title:matches:"test.alert","test,notification"',
                Filter(negation=False, path=["title"], operator="matches", values=["test.alert", "test,notification"]),
            ),
            (
                "not(title:matches:Test alert,testsite,Test notification)",
                Filter(
                    negation=True,
                    path=["title"],
                    operator="matches",
                    values=["Test alert", "testsite", "Test notification"],
                ),
            ),
        ]
        for i, case in enumerate(cases):
            filter_str, expected_filter_obj = case
            with self.subTest(i=i, case=filter_str):
                self.assertEqual(Filter.parse(filter_str), expected_filter_obj)

    def test_str(self) -> None:
        cases = [
            (
                Filter(negation=False, path=["priority", "summary"], operator="matches", values=["p1", "p2"]),
                "priority.summary:matches:p1,p2",
            ),
            (
                Filter(
                    negation=True,
                    path=["title"],
                    operator="matches",
                    values=["test alert", "testsite", "test notification"],
                ),
                "not(title:matches:test alert,testsite,test notification)",
            ),
        ]
        for i, case in enumerate(cases):
            filter_obj, expected_filter_str = case
            with self.subTest(i=i, case=expected_filter_str):
                self.assertEqual(str(filter_obj), expected_filter_str)

    def test__get_value(self) -> None:
        data = {
            "title": "Non test alert",
            "priority": {
                "obj": {},
                "summary": "P1",
                "nullable": None,
                "details": {"key": {"key": "value"}},
            },
            "arr": [{"key": "value"}, 10, "test"],
            "id": 11,
            "score": 15.4,
            "empty": "",
            "bool": True,
        }

        cases = [
            ("title", "Non test alert"),
            ("priority.obj", {}),
            ("priority.summary", "P1"),
            ("priority.nullable", None),
            ("priority.details.key.key", "value"),
            ("arr", data["arr"]),
            ("arr.-1", None),
            ("arr.0", {"key": "value"}),
            ("arr.0.key", "value"),
            ("arr.1", 10),
            ("arr.2", "test"),
            ("arr.3", None),
            ("id", 11),
            ("score", 15.4),
            ("empty", ""),
            ("bool", True),
            ("", data),
            (".", data),
        ]

        for i, case in enumerate(cases):
            path, expected_value = case
            with self.subTest(i=i, case=path):
                filter_str = f"{path}:matches:test"
                filter_obj = Filter.parse(filter_str)
                self.assertEqual(filter_obj._get_value(data), expected_value)

    def test_check(self) -> None:
        data = {
            "title": "Non Test alert",
            "priority": {
                "obj": {},
                "summary": "P1",
                "nullable": None,
                "details": {"key": {"key": "value"}},
            },
            "arr": [{"key": "value"}, 10, "test"],
            "emptyArr": [],
            "id": 11,
            "score": 15.4,
            "empty": "",
            "boolTrue": True,
            "boolFalse": False,
            "zero": 0,
        }

        cases = [
            # The path exists, the value is a string.
            ("priority.summary:matches", True),
            ("priority.summary:matches:^P1$,^P2$", True),
            ("priority.summary:matches:^P3$", False),
            ("priority.summary:matches:^P1|P2$", True),
            ("title:matches:Test alert,testsite,Test notification", True),
            ("not(title:matches:Test alert,testsite,Test notification)", False),
            # The path exists, the value is a false-like value - empty string.
            ("empty:matches", False),
            ("empty:matches:", False),
            ("empty:matches:.", False),
            # The path exists, the value is an integer.
            ("id:matches", True),
            ("id:matches:", True),
            ("id:matches:.", True),
            ("id:matches:11", True),
            ("id:matches:1", True),
            ("id:matches:^1$", False),
            ("id:matches:^11$", True),
            # The path exists, the value is false-like value - zero.
            ("zero:matches", False),
            ("zero:matches:", False),
            ("zero:matches:.", True),
            ("zero:matches:0", True),
            ("zero:matches:1", False),
            # The path exists, the value is a float.
            ("score:matches", True),
            ("score:matches:", True),
            ("score:matches:.", True),
            ("score:matches:15.", True),
            ("score:matches:^15\\.4$", True),
            ("score:matches:^15$", False),
            # The path exists, the value is a bool.
            ("boolTrue:matches", True),
            ("boolTrue:matches:", True),
            ("boolTrue:matches:.", True),
            ("boolTrue:matches:true", True),
            ("boolTrue:matches:false", False),
            ("boolTrue:matches:0", False),
            # The path exists, the value is false-like value - false.
            ("boolFalse:matches", False),
            ("boolFalse:matches:", False),
            ("boolFalse:matches:.", True),
            ("boolFalse:matches:false", True),
            ("boolFalse:matches:true", False),
            ("boolFalse:matches:0", False),
            # The path exists, the value is a dict.
            ("priority.details:matches", True),
            ("priority.details:matches:", True),
            ("priority.details:matches:.", True),
            ('priority.details:matches:"value"', True),
            ('priority.details:matches:"value0"', False),
            # The path exists, the value is a false-like value - empty dict.
            ("priority.obj:matches", False),
            ("priority.obj:matches:", False),
            ("priority.obj:matches:.", True),
            # The path exists, the value is a list.
            ("arr:matches", True),
            ("arr:matches:", True),
            ("arr:matches:.", True),
            ('arr:matches:"test"', True),
            ('arr:matches:"test0"', False),
            # The path exists, the value is a false-like value - empty list.
            ("emptyArr:matches", False),
            ("emptyArr:matches:", False),
            ("emptyArr:matches:.", True),
            # The path exists, the value is a false-like value - null.
            ("priority.nullable:matches", False),
            ("priority.nullable:matches:", False),
            ("priority.nullable:matches:.", True),
            # The path does not exist.
            ("the.path.does.not.exist:matches", False),
            ("the.path.does.not.exist:matches:", False),
            ("the.path.does.not.exist:matches:.", True),
        ]
        for i, case in enumerate(cases):
            filter_str, expected_result = case
            with self.subTest(i=i, case=case):
                filter_obj = Filter.parse(filter_str)
                self.assertEqual(filter_obj.check(str(case), data), expected_result)


if __name__ == "__main__":
    unittest.main()
