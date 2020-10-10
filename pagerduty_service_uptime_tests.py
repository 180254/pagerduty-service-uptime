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


if __name__ == '__main__':
    unittest.main()
