import unittest

from iso8601 import parse_date

from pagerduty_service_uptime import *


class TestIncidentsOverlap(unittest.TestCase):

    def test_not_overlapping_one_second(self):
        self.assertFalse(incidents_overlap(
            Incident([], parse_date('2020-10-01 10:00:00'), parse_date('2020-10-01 11:00:00')),
            Incident([], parse_date('2020-10-01 11:00:01'), parse_date('2020-10-01 12:00:00'))
        ))

    def test_not_overlapping_one_hour(self):
        self.assertFalse(incidents_overlap(
            Incident([], parse_date('2020-10-01 10:00:00'), parse_date('2020-10-01 11:00:00')),
            Incident([], parse_date('2020-10-01 12:00:00'), parse_date('2020-10-01 12:00:01'))
        ))

    def test_not_overlapping_one_day(self):
        self.assertFalse(incidents_overlap(
            Incident([], parse_date('2020-10-01 10:00:00'), parse_date('2020-10-01 11:00:00')),
            Incident([], parse_date('2020-10-02 10:00:00'), parse_date('2020-10-02 11:00:00')),
        ))

    def test_overlapping_same_second(self):
        self.assertTrue(incidents_overlap(
            Incident([], parse_date('2020-10-01 10:00:00'), parse_date('2020-10-01 11:00:00')),
            Incident([], parse_date('2020-10-01 10:00:00'), parse_date('2020-10-01 12:00:00')),
        ))

    def test_overlapping_one_second(self):
        self.assertTrue(incidents_overlap(
            Incident([], parse_date('2020-10-01 10:00:00'), parse_date('2020-10-01 11:00:01')),
            Incident([], parse_date('2020-10-01 11:00:00'), parse_date('2020-10-01 12:00:00')),
        ))

    def test_overlapping_one_hour(self):
        self.assertTrue(incidents_overlap(
            Incident([], parse_date('2020-10-01 10:00:00'), parse_date('2020-10-01 12:00:00')),
            Incident([], parse_date('2020-10-01 11:00:00'), parse_date('2020-10-01 12:05:00')),
        ))

    def test_overlapping_second_incident_ends_before_first(self):
        self.assertTrue(incidents_overlap(
            Incident([], parse_date('2020-10-01 10:00:00'), parse_date('2020-10-01 12:00:00')),
            Incident([], parse_date('2020-10-01 11:00:00'), parse_date('2020-10-01 11:05:00')),
        ))


class TestMergeTwoIncidents(unittest.TestCase):
    def test_overlapping_same_second(self):
        self.assertEqual(
            merge_two_incidents(
                Incident([10, 13], parse_date('2020-10-01 10:00:00'), parse_date('2020-10-01 11:00:00')),
                Incident([8], parse_date('2020-10-01 10:00:00'), parse_date('2020-10-01 12:00:00')),
            ),
            Incident([10, 13, 8], parse_date('2020-10-01 10:00:00'), parse_date('2020-10-01 12:00:00')),
        )

    def test_overlapping_one_second(self):
        self.assertEqual(
            merge_two_incidents(
                Incident([9], parse_date('2020-10-01 10:00:00'), parse_date('2020-10-01 11:00:01')),
                Incident([1, 3], parse_date('2020-10-01 11:00:00'), parse_date('2020-10-01 12:00:00')),
            ),
            Incident([9, 1, 3], parse_date('2020-10-01 10:00:00'), parse_date('2020-10-01 12:00:00')),
        )

    def test_overlapping_one_hour(self):
        self.assertEqual(
            merge_two_incidents(
                Incident([1], parse_date('2020-10-01 10:00:00'), parse_date('2020-10-01 12:00:00')),
                Incident([2], parse_date('2020-10-01 11:00:00'), parse_date('2020-10-01 12:05:00')),
            ),
            Incident([1, 2], parse_date('2020-10-01 10:00:00'), parse_date('2020-10-01 12:05:00')),
        )

    def test_overlapping_second_incident_ends_before_first(self):
        self.assertEqual(
            merge_two_incidents(
                Incident([1, 3, 4], parse_date('2020-10-01 10:00:00'), parse_date('2020-10-01 12:00:00')),
                Incident([2, 5, 6], parse_date('2020-10-01 11:00:00'), parse_date('2020-10-01 11:05:00')),
            ),
            Incident([1, 3, 4, 2, 5, 6], parse_date('2020-10-01 10:00:00'), parse_date('2020-10-01 12:00:00')),
        )


class TestIntervalsGen(unittest.TestCase):

    def test1(self):
        intervals = list(intervals_gen(parse_date("2019-01-01 00:00:00"),
                                       parse_date("2020-01-01 00:00:00"),
                                       parse_relativedelta("6 months")))
        self.assertListEqual(intervals, [
            (parse_date("2019-01-01 00:00:00"), parse_date("2019-07-01 00:00:00")),
            (parse_date("2019-07-01 00:00:00"), parse_date("2020-01-01 00:00:00")),
        ])

    def test2(self):
        intervals = list(intervals_gen(parse_date("2019-01-01 00:00:00"),
                                       parse_date("2019-12-31 23:59:59"),
                                       parse_relativedelta("6 months")))
        self.assertListEqual(intervals, [
            (parse_date("2019-01-01 00:00:00"), parse_date("2019-07-01 00:00:00")),
            (parse_date("2019-07-01 00:00:00"), parse_date("2019-12-31 23:59:59")),
        ])

    def test3(self):
        intervals = list(intervals_gen(parse_date("2018-01-01 00:00:00"),
                                       parse_date("2020-01-01 00:00:00"),
                                       parse_relativedelta("1 year")))
        self.assertListEqual(intervals, [
            (parse_date("2018-01-01 00:00:00"), parse_date("2019-01-01 00:00:00")),
            (parse_date("2019-01-01 00:00:00"), parse_date("2020-01-01 00:00:00")),
        ])

    def test4(self):
        intervals = list(intervals_gen(parse_date("2018-01-01 00:00:00"),
                                       parse_date("2019-01-01 00:00:00"),
                                       parse_relativedelta("1 month")))
        self.assertListEqual(intervals, [
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
        ])

    def test5(self):
        intervals = list(intervals_gen(parse_date("2018-01-01 10:00:05"),
                                       parse_date("2019-01-01 00:00:00"),
                                       parse_relativedelta("1 month")))
        self.assertListEqual(intervals, [
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
        ])

    def test6(self):
        intervals = list(intervals_gen(parse_date("2019-01-01 00:00:00"),
                                       parse_date("2020-01-01 00:00:00"),
                                       parse_relativedelta("15 months")))
        self.assertListEqual(intervals, [
            (parse_date("2019-01-01 00:00:00"), parse_date("2020-01-01 00:00:00")),
        ])


class MergeOverlappingIncidents(unittest.TestCase):

    def test1(self):
        merged_incidents = merge_overlapping_incidents([
            Incident([1], parse_date('2020-10-01 10:00:00'), parse_date('2020-10-01 11:00:00')),
            Incident([2], parse_date('2020-10-01 11:00:00'), parse_date('2020-10-01 11:00:01')),
            Incident([3], parse_date('2020-10-01 11:00:01'), parse_date('2020-10-01 11:00:02')),
            Incident([4], parse_date('2020-10-01 12:00:00'), parse_date('2020-10-01 12:00:05')),
            Incident([5], parse_date('2020-10-01 12:00:06'), parse_date('2020-10-01 13:00:00')),
            Incident([6], parse_date('2020-10-01 12:00:07'), parse_date('2020-10-01 12:30:00')),
            Incident([7], parse_date('2020-10-01 13:00:00'), parse_date('2020-10-01 13:00:00')),
            Incident([8], parse_date('2020-10-01 14:00:00'), parse_date('2020-10-01 14:00:01')),
            Incident([9], parse_date('2020-10-01 14:00:00'), parse_date('2020-10-01 14:10:10')),
            Incident([10], parse_date('2020-10-01 14:00:00'), parse_date('2020-10-01 14:05:00')),
            Incident([11], parse_date('2020-10-01 15:00:00'), parse_date('2020-10-01 15:05:00')),
            Incident([12], parse_date('2020-10-01 15:01:00'), parse_date('2020-10-01 15:01:05')),
        ])
        self.assertListEqual(merged_incidents, [
            Incident([1, 2, 3], parse_date("2020-10-01 10:00:00"), parse_date("2020-10-01 11:00:02")),
            Incident([4], parse_date("2020-10-01 12:00:00"), parse_date("2020-10-01 12:00:05")),
            Incident([5, 6, 7], parse_date("2020-10-01 12:00:06"), parse_date("2020-10-01 13:00:00")),
            Incident([8, 9, 10], parse_date("2020-10-01 14:00:00"), parse_date("2020-10-01 14:10:10")),
            Incident([11, 12], parse_date("2020-10-01 15:00:00"), parse_date("2020-10-01 15:05:00"))
        ])


if __name__ == '__main__':
    unittest.main()
