#!/usr/bin/env python3

import argparse
import functools
import itertools
import logging
import re
import sys
import time
from datetime import datetime, timedelta
from typing import List, Pattern, AnyStr, Optional, Generator, Tuple

import iso8601
import requests
from dateutil.relativedelta import relativedelta


class Incident:
    def __init__(self, ids, created, resolved):
        self.ids = ids[:]
        self.created = created
        self.resolved = resolved

    @functools.lru_cache(maxsize=1024)
    def __str__(self):
        return "({},{},{})".format(self.ids, self.created.isoformat(), self.resolved.isoformat())

    def __repr__(self):
        return self.__str__()

    @functools.lru_cache(maxsize=1024)
    def total_seconds(self):
        return (self.resolved - self.created).total_seconds()

    def __hash__(self) -> int:
        return hash((tuple(self.ids), self.created, self.resolved))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Incident):
            return self.ids == other.ids \
                   and self.created == other.created \
                   and self.resolved == other.resolved
        return False


# Function that checks if an incident indicates a service outage.
# Incidents generated by StatusCake starts with "Website | Your site".
def is_outage(title_checks: Optional[List[Pattern[AnyStr]]], title: str) -> bool:
    if title_checks is None:
        return True
    for title_check in title_checks:
        if re.search(title_check, title):
            return True
    return False


# https://api-reference.pagerduty.com/#!/Incidents/get_incidents
def collect_incidents_from_pagerduty(session: requests.Session,
                                     api_token: str,
                                     service_ids: List[str],
                                     title_checks: Optional[List[Pattern[AnyStr]]],
                                     start_date: datetime,
                                     end_date: datetime,
                                     offset: int = 0) -> List[Incident]:
    logging.info("collect_incidents_from_pagerduty({},{},{})"
                 .format(start_date.isoformat(), end_date.isoformat(), offset))

    response = session.get(
        "https://api.pagerduty.com/incidents",
        params={'since': start_date.isoformat(),
                "until": end_date.isoformat(),  # until is exclusive
                "service_ids[]": service_ids,
                "statuses[]": "resolved",
                "time_zone": "UTC",
                "sort_by": "created_at",
                "limit": 100,
                "offset": offset},
        headers={"Accept": "application/vnd.pagerduty+json;version=2",
                 "Authorization": "Token token={}".format(api_token)})

    # https://v2.developer.pagerduty.com/docs/rate-limiting
    if response.status_code == 429:
        response.close()
        logging.info("request has been throttled, sleeping 1 second.")
        time.sleep(1)
        return collect_incidents_from_pagerduty(
            session, api_token, service_ids, title_checks, start_date, end_date, offset)

    if response.status_code != 200:
        response.close()
        raise requests.HTTPError("response.status_code is {}, != 200".format(response.status_code))

    api_response = response.json()
    response.close()

    incidents = []
    for api_incident in api_response["incidents"]:
        incident_id = api_incident["id"]

        title = api_incident["title"].replace('\r', '').replace('\n', ' ')
        if not is_outage(title_checks, title):
            logging.debug("incident rejected: {} {}".format(incident_id, title))
            continue
        logging.debug("incident accepted: {} {}".format(incident_id, title))

        created_at = iso8601.parse_date(api_incident["created_at"])
        last_status_change_at = iso8601.parse_date(api_incident["last_status_change_at"])
        incidents.append(Incident([incident_id], created_at, last_status_change_at))

    # https://v2.developer.pagerduty.com/docs/pagination
    if api_response["more"]:
        next_offset = api_response["offset"] + len(api_response["incidents"])
        more_incidents = collect_incidents_from_pagerduty(
            session, api_token, service_ids, title_checks, start_date, end_date, next_offset)
        incidents.extend(more_incidents)

    return incidents


# Find all incidents which overlap, and merge them.
# The resulting list includes only unique incidents that times do not overlap.
# Note: "incidents" array must be sorted by "created asc".
def merge_overlapping_incidents(incidents: List[Incident]) -> List[Incident]:
    ret = incidents[:]
    performed_any_merge = True
    while performed_any_merge:
        performed_any_merge = False
        for i in range(len(ret) - 1, 0, -1):
            if incidents_overlap(ret[i - 1], ret[i]):
                ret[i - 1] = merge_two_incidents(ret[i - 1], ret[i])
                ret.pop(i)
                performed_any_merge = True

    # Above algorithm should works. Lets check it ;)
    for i in range(len(ret)):
        for j in range(len(ret)):
            if i != j and incidents_overlap(ret[i], ret[j]):
                raise AssertionError("Omg. {} and {} ({} and {}) overlaps! It's script bug."
                                     .format(i, j, ret[i], ret[j]))
    return ret


# Function checks whether two incidents overlap.
# Math: https://nedbatchelder.com/blog/201310/range_overlap_in_two_compares.html
def incidents_overlap(incident_a: Incident, incident_b: Incident) -> bool:
    return incident_a.resolved >= incident_b.created and incident_b.resolved >= incident_a.created


# "Merge" means interval of new incident is union of intervals of two input *overlapping* incidents.
def merge_two_incidents(incident_a: Incident, incident_b: Incident) -> Incident:
    logging.debug("merge_incidents({},{})".format(incident_a, incident_b))
    new_ids = incident_a.ids + incident_b.ids
    new_created = min(incident_a.created, incident_b.created)
    new_resolved = max(incident_a.resolved, incident_b.resolved)
    new_incident = Incident(new_ids, new_created, new_resolved)
    return new_incident


# Filter incidents, return the ones in the interval [start_date, end_date).
# Note: Incident "created" date is compared.
def filter_incidents(start_date: datetime, end_date: datetime, all_incidents: List[Incident]) -> List[Incident]:
    try:
        first_matching_index = next(i for i, v in enumerate(all_incidents) if v.created >= start_date)
    except StopIteration:
        first_matching_index = len(all_incidents)
    try:
        first_mismatched_index = next(i for i, v in enumerate(all_incidents) if v.created >= end_date)
    except StopIteration:
        first_mismatched_index = len(all_incidents)

    interval_incidents = all_incidents[first_matching_index:first_mismatched_index]
    return interval_incidents


# Print final report about uptime.
def report_uptime(start_date: datetime, end_date: datetime, interval_incidents: List[Incident]) -> type(None):
    interval_duration = (end_date - start_date).total_seconds()
    interval_downtime = sum(map(lambda inc: inc.total_seconds(), interval_incidents))
    interval_uptime = (1 - (interval_downtime / interval_duration)) * 100
    interval_ids = list(itertools.chain.from_iterable(map(
        lambda inc: [inc.ids] if len(inc.ids) > 1 else inc.ids, interval_incidents)))
    end_date_inclusive = end_date - relativedelta(seconds=1)

    logging.warning("From: {} To: {} Uptime: {:6.2f} Incidents: {:3} Downtime: {: >8} Incidents: {}"
                    .format(start_date.isoformat(),
                            end_date_inclusive.isoformat(),
                            interval_uptime,
                            len(interval_incidents),
                            str(timedelta(seconds=interval_downtime)),
                            interval_ids)
                    )


# Generate sub-intervals from start_date (inclusive) to end_date (exclusive) with relative_delta step.
# Example: start_date=2019-01-01, end_date=2020-01-01, relative_delta=6months will return two sub-intervals:
#          [2019-01-01 00:00:00, 2019-07-01 00:00:00)
#          [2019-07-01 00:00:00, 2020-01-01 00:00:00)
def intervals_gen(start_date: datetime, end_date: datetime, relative_delta: relativedelta) \
        -> Generator[Tuple[datetime, datetime], None, None]:
    interval_since = start_date
    while interval_since < end_date:
        interval_until = interval_since + relative_delta
        if interval_until > end_date:
            interval_until = end_date
        yield interval_since, interval_until
        interval_since = interval_until


def parse_service_id(string: str) -> str:
    match = re.search(r'https://[a-zA-Z0-9.-_]*pagerduty\.com/services/([a-zA-Z0-9]+)', string)
    if match:
        return match.group(1)
    return string


def parse_relativedelta(string: str) -> relativedelta:
    match = re.match(r"^(\d+) *(hour|day|month|year)s?$", string)
    if match is None:
        raise ValueError(f"Invalid relative data string: {string}.")
    num = int(match.group(1))
    unit = match.group(2)
    if unit == "hour":
        return relativedelta(hours=num)
    if unit == "day":
        return relativedelta(days=num)
    if unit == "month":
        return relativedelta(months=num)
    if unit == "year":
        return relativedelta(years=num)
    raise AssertionError("Should never occur. It's script bug.")


# Go go, Power Rangers!
def main() -> int:
    # args
    argparser = argparse.ArgumentParser(description="pagerduty-service-uptime")
    argparser.add_argument(
        "--log-level",
        metavar="LOGLEVEL",
        dest="log_level",
        type=str,
        required=True,
        help="one of CRITICAL, ERROR, WARN, INFO, DEBUG")
    argparser.add_argument(
        "--api-token",
        metavar="APITOKEN",
        dest="api_token",
        type=str,
        required=True,
        help="personal rest api key, "
             "check https://support.pagerduty.com/docs/generating-api-keys#section-generating-a-personal-rest-api-key")
    argparser.add_argument(
        "--service-ids",
        metavar="SERVICEID",
        dest="service_ids",
        type=parse_service_id,
        nargs="+",
        required=True,
        help="service id (e.g. ABCDEF4) or service url (e.g. https://some.pagerduty.com/services/ABCDEF4); "
             "takes several values, which will be coordinated using OR")
    argparser.add_argument(
        "--title-checks",
        metavar="PATTERN",
        dest="title_checks",
        type=re.compile,
        nargs="*",
        required=False,
        help="incident title check pattern, eg. '^Downtime'; "
             "the title of the incident must match a given regular expressions to be taken into account; "
             "takes several values, which will be coordinated using OR; "
             "if not specified, it means each incident affects uptime calculation")
    argparser.add_argument(
        "--incidents-since",
        metavar="ISODATE",
        dest="incidents_since",
        type=iso8601.parse_date,
        required=True,
        help="beginning (inclusive) of the reported time range; "
             "iso8601 date, e.g. 2019-01-01T00:00:00Z")
    argparser.add_argument(
        "--incidents-until",
        metavar="ISODATE",
        dest="incidents_until",
        type=iso8601.parse_date,
        required=True,
        help="end (exclusive) of the reported time range; "
             "iso8601 date, e.g. 2020-01-01T00:00:00Z")
    argparser.add_argument(
        "--report-step",
        metavar="STEP",
        dest="report_step",
        type=parse_relativedelta,
        required=True,
        help="report step, must match '(\\d+) (hour|day|month|year)s', e.g. 1 month")
    argparser.add_argument(
        '--version',
        action='version',
        version='%(prog)s 2.0.0')
    args = argparser.parse_args()

    # logging
    logging.basicConfig(stream=sys.stdout, level=args.log_level, format="%(message)s")
    logging.info("log_level={}".format(logging.getLevelName(args.log_level)))
    logging.info("api_token={}...".format(args.api_token[:3]))
    logging.info("service_ids={}".format(args.service_ids))
    logging.info("title_checks={}".format(args.title_checks))
    logging.info("incidents_since={}".format(args.incidents_since))
    logging.info("incidents_until={}".format(args.incidents_until))
    logging.info("report_step={}".format(args.report_step))

    # main logic
    incidents = []
    with requests.Session() as requests_session:
        collect_step = relativedelta(months=4)
        for interval_since, interval_until in intervals_gen(args.incidents_since, args.incidents_until, collect_step):
            collected_incidents = \
                collect_incidents_from_pagerduty(requests_session, args.api_token, args.service_ids, args.title_checks,
                                                 interval_since, interval_until)
            incidents.extend(collected_incidents)
    incidents.sort(key=lambda inc: (inc.created, -inc.total_seconds()))
    logging.info("len(incidents)={}".format(len(incidents)))

    merged_incidents = merge_overlapping_incidents(incidents)
    logging.info("len(merged_incidents)={}".format(len(merged_incidents)))
    logging.debug("merged_incidents={}".format(str(merged_incidents)))

    for interval_since, interval_until in intervals_gen(args.incidents_since, args.incidents_until, args.report_step):
        interval_incidents = filter_incidents(interval_since, interval_until, merged_incidents)
        report_uptime(interval_since, interval_until, interval_incidents)

    return 0


if __name__ == '__main__':
    sys.exit(main())
