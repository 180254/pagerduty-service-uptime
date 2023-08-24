#!venv/bin/python3

import unittest

from pagerduty_service_uptime import (
    Alert,
    alerts_overlap,
    filter_alerts,
    intervals_gen,
    merge_overlapping_alerts,
    merge_two_alerts,
    parse_date,
    parse_relativedelta,
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
                parse_date("2019-01-01 00:00:00"), parse_date("2020-01-01 00:00:00"), parse_relativedelta("6 months")
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
                parse_date("2019-01-01 00:00:00"), parse_date("2019-12-31 23:59:59"), parse_relativedelta("6 months")
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
                parse_date("2018-01-01 00:00:00"), parse_date("2020-01-01 00:00:00"), parse_relativedelta("1 year")
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
                parse_date("2018-01-01 00:00:00"), parse_date("2019-01-01 00:00:00"), parse_relativedelta("1 month")
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
                parse_date("2018-01-01 10:00:05"), parse_date("2019-01-01 00:00:00"), parse_relativedelta("1 month")
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
                parse_date("2019-01-01 00:00:00"), parse_date("2020-01-01 00:00:00"), parse_relativedelta("15 months")
            )
        )
        self.assertListEqual(
            intervals,
            [
                (parse_date("2019-01-01 00:00:00"), parse_date("2020-01-01 00:00:00")),
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


if __name__ == "__main__":
    unittest.main()
