#!/usr/bin/env python3

import argparse
import itertools
import logging
import re
import sys
import time
from concurrent import futures
from concurrent.futures import Future
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import List, Pattern, AnyStr, Optional, Generator, Tuple, Union, Dict

import requests
from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta
from diskcache import Cache

VERSION = "3.0.0"


class Alert:
    def __init__(self, ids: List[Union[int, str]], created: datetime, resolved: datetime):
        self.ids: List[Union[int, str]] = ids[:]
        self.created: datetime = created
        self.resolved: datetime = resolved

    def total_seconds(self):
        return (self.resolved - self.created).total_seconds()

    def __str__(self):
        return f"({self.ids},{self.created.isoformat()},{self.resolved.isoformat()})"

    def __repr__(self):
        return self.__str__()

    def __hash__(self) -> int:
        return hash((tuple(self.ids), self.created, self.resolved))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Alert):
            return self.ids == other.ids \
                   and self.created == other.created \
                   and self.resolved == other.resolved
        return False


# Call PageDuty API.
# Handles repeating errors and pagination.
# Extracts all items for a given collector_key.
def call_pagerduty_api(call_id,
                       session: requests.Session,
                       api_token: str,
                       url: str,
                       params: Dict,
                       collector_key: str,
                       offset: int = 0,
                       retry: int = 0) -> List[Dict]:
    logging.info(f"call_pagerduty_api({call_id},{offset},{retry})")

    response = None
    try:
        params_with_pagination = {**params, "limit": 100, "offset": offset}
        headers = {
            "Accept": "application/vnd.pagerduty+json;version=2",
            "Authorization": f"Token token={api_token}"
        }

        response = session.get(url, params=params_with_pagination, headers=headers)

        if response.status_code != 200:
            if retry < 3:
                logging.info(f"response.status_code is {response.status_code}, != 200, repeating")
                # https://v2.developer.pagerduty.com/docs/rate-limiting
                if response.status_code == 429:
                    time.sleep(1)
                return call_pagerduty_api(call_id, session, api_token, url, params, collector_key, offset, retry + 1)
            raise requests.HTTPError(f"response.status_code is {response.status_code}, != 200")

        result = []
        api_response = response.json()
        result.extend(api_response[collector_key])

        # https://v2.developer.pagerduty.com/docs/pagination
        if api_response["more"]:
            next_offset = api_response["offset"] + len(api_response[collector_key])
            more_results = call_pagerduty_api(call_id, session, api_token, url, params, collector_key, next_offset, 0)
            result.extend(more_results)

        return result
    finally:
        if response is not None:
            response.close()


# https://developer.pagerduty.com/api-reference/reference/REST/openapiv3.json/paths/~1incidents/get
def call_pagerduty_list_incidents(session: requests.Session,
                                  api_token: str,
                                  service_ids: List[str],
                                  title_checks: Optional[List[Pattern[AnyStr]]],
                                  start_date: datetime,
                                  end_date: datetime) -> List[str]:
    api_incidents = call_pagerduty_api(
        f"https://api.pagerduty.com/incidents,since={start_date.isoformat()},until={end_date.isoformat()}",
        session,
        api_token,
        "https://api.pagerduty.com/incidents",
        {
            "since": start_date.isoformat(),
            "until": end_date.isoformat(),  # until is exclusive
            "service_ids[]": service_ids,
            "statuses[]": "resolved",
            "time_zone": "UTC",
            "sort_by": "created_at",
        },
        "incidents")

    incidents = []
    for api_incident in api_incidents:
        incident_id = api_incident["id"]

        incident_title = api_incident["title"].replace("\t", " ").replace("\r", " ").replace("\n", " ").strip()
        if not is_outage(title_checks, incident_title):
            logging.debug("incident rejected: {} {}".format(incident_id, incident_title))
            continue
        logging.debug("incident accepted: {} {}".format(incident_id, incident_title))

        incidents.append(incident_id)

    return incidents


# https://developer.pagerduty.com/api-reference/reference/REST/openapiv3.json/paths/~1incidents~1%7Bid%7D~1alerts/get
def call_pagerduty_list_alerts_for_an_incident(cache: Cache,
                                               session: requests.Session,
                                               api_token: str,
                                               incident_id: str) -> List[Alert]:
    # Method result are considered stable and cached on disk.
    # Script is processing only resolved incidents.
    cache_item_id = f"alerts_for_an_incident-{incident_id}"

    if cache_item_id in cache:
        return cache.get(cache_item_id)

    api_alerts = call_pagerduty_api(
        f"https://api.pagerduty.com/incidents/{incident_id}/alerts",
        session,
        api_token,
        f"https://api.pagerduty.com/incidents/{incident_id}/alerts",
        {
            "statuses[]": "resolved",
            "time_zone": "UTC"
        },
        "alerts")

    alerts = list(map(lambda api_alert:
                      Alert(
                          [f"{incident_id}/{api_alert['id']}"],
                          parse_date(api_alert["created_at"]),
                          parse_date(api_alert["resolved_at"])),
                      api_alerts))

    cache.set(cache_item_id, alerts)
    return alerts


# Find all alerts which overlap, and merge them.
# The resulting list includes only unique alerts that times do not overlap.
# Note: "alerts" array must be sorted by "created asc".
def merge_overlapping_alerts(alerts: List[Alert]) -> List[Alert]:
    ret = alerts[:]
    performed_any_merge = True
    while performed_any_merge:
        performed_any_merge = False
        for i in range(len(ret) - 1, 0, -1):
            if alerts_overlap(ret[i - 1], ret[i]):
                ret[i - 1] = merge_two_alerts(ret[i - 1], ret[i])
                ret.pop(i)
                performed_any_merge = True

    # Above algorithm should works. Lets check.
    for i in range(len(ret)):
        for j in range(len(ret)):
            if i != j and alerts_overlap(ret[i], ret[j]):
                raise AssertionError("Omg. {} and {} ({} and {}) overlaps! It's script bug."
                                     .format(i, j, ret[i], ret[j]))
    return ret


# Function checks whether two alerts overlap.
# Math: https://nedbatchelder.com/blog/201310/range_overlap_in_two_compares.html
def alerts_overlap(alert_a: Alert, alert_b: Alert) -> bool:
    return alert_a.resolved >= alert_b.created and alert_b.resolved >= alert_a.created


# "Merge" means interval of new alert is union of intervals of two input *overlapping* alerts.
def merge_two_alerts(alert_a: Alert, alert_b: Alert) -> Alert:
    logging.debug("merge_two_alerts({},{})".format(alert_a, alert_b))
    new_ids = alert_a.ids + alert_b.ids
    new_created = min(alert_a.created, alert_b.created)
    new_resolved = max(alert_a.resolved, alert_b.resolved)
    new_alert = Alert(new_ids, new_created, new_resolved)
    return new_alert


# Filter alerts, return the ones in the interval [start_date, end_date).
# Note: Alert "created" date is compared.
def filter_alerts(start_date: datetime, end_date: datetime, all_alerts: List[Alert]) -> List[Alert]:
    try:
        first_matching_index = next(i for i, v in enumerate(all_alerts) if v.created >= start_date)
    except StopIteration:
        first_matching_index = len(all_alerts)
    try:
        first_mismatched_index = next(i for i, v in enumerate(all_alerts) if v.created >= end_date)
    except StopIteration:
        first_mismatched_index = len(all_alerts)

    interval_alerts = all_alerts[first_matching_index:first_mismatched_index]
    return interval_alerts


# Print final report about uptime.
def report_uptime(start_date: datetime,
                  end_date: datetime,
                  interval_alerts: List[Alert],
                  report_details_level: int) -> type(None):
    interval_duration = (end_date - start_date).total_seconds()
    interval_downtime = sum(map(lambda inc: inc.total_seconds(), interval_alerts))
    interval_uptime = (1 - (interval_downtime / interval_duration)) * 100
    interval_ids = list(itertools.chain.from_iterable(map(
        lambda inc: [inc.ids] if len(inc.ids) > 1 else inc.ids, interval_alerts)))
    end_date_inclusive = end_date - relativedelta(seconds=1)

    report_msg = ""
    if report_details_level == 0:
        report_msg = "From: {} To: {} Uptime: {:6.2f} Incidents: {:3} Downtime: {: >8}"
    elif report_details_level == 1:
        report_msg = "From: {} To: {} Uptime: {:6.2f} Incidents: {:3} Downtime: {: >8} Incidents: {}"

    logging.warning(
        report_msg.format(start_date.isoformat(),
                          end_date_inclusive.isoformat(),
                          interval_uptime,
                          len(interval_alerts),
                          str(timedelta(seconds=interval_downtime)),
                          interval_ids)
    )


# Function that checks if an incident indicates a service outage.
# Incidents generated by StatusCake starts with "Website | Your site".
def is_outage(title_checks: Optional[List[Pattern[AnyStr]]], title: str) -> bool:
    if title_checks is None:
        return True
    for title_check in title_checks:
        if re.search(title_check, title):
            return True
    return False


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
    match = re.search(r"https://[a-zA-Z0-9.-_]*pagerduty\.com/services/([a-zA-Z0-9]+)", string)
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


def main() -> int:
    # args
    argparser = argparse.ArgumentParser(description="pagerduty-service-uptime")
    argparser.add_argument(
        "--log-level",
        metavar="LOGLEVEL",
        dest="log_level",
        type=str,
        required=True,
        help="one of CRITICAL, ERROR, WARN, INFO, DEBUG, NOTSET")
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
        type=parse_date,
        required=True,
        help="beginning (inclusive) of the reported time range; "
             "iso8601 date, e.g. 2019-01-01T00:00:00Z")
    argparser.add_argument(
        "--incidents-until",
        metavar="ISODATE",
        dest="incidents_until",
        type=parse_date,
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
        "--report-details-level",
        metavar="LVL",
        dest="report_details_level",
        type=int,
        choices=[0, 1],
        required=False,
        default=0,
        help="number of details in the report; from 0 to 1; higher = more details")
    argparser.add_argument(
        "--version",
        action="version",
        version=("%(prog)s " + VERSION))
    args = argparser.parse_args()

    # logging
    logging.basicConfig(stream=sys.stdout, level=args.log_level, format="%(levelname)s %(message)s")
    logging.info("log_level={}".format(logging.getLevelName(args.log_level)))
    logging.info("api_token={}...".format(args.api_token[:3]))
    logging.info("service_ids={}".format(args.service_ids))
    logging.info("title_checks={}".format(args.title_checks))
    logging.info("incidents_since={}".format(args.incidents_since))
    logging.info("incidents_until={}".format(args.incidents_until))
    logging.info("report_step={}".format(args.report_step))

    # collect incidents
    incidents: List[str] = []
    with requests.Session() as requests_session:
        collect_step = relativedelta(months=4)
        for interval_since, interval_until in intervals_gen(args.incidents_since, args.incidents_until, collect_step):
            collected_incidents = \
                call_pagerduty_list_incidents(requests_session, args.api_token, args.service_ids, args.title_checks,
                                              interval_since, interval_until)
            incidents.extend(collected_incidents)

    # collect alerts
    original_alerts: List[Alert] = []
    simplified_alerts: List[Alert] = []
    alerts_futures: List[Future[List[Alert]]] = []
    with ThreadPoolExecutor(max_workers=8) as executor, \
            Cache(".cache") as cache, \
            requests.Session() as requests_session:
        for incident_id in incidents:
            future = executor.submit(
                call_pagerduty_list_alerts_for_an_incident, cache, requests_session, args.api_token, incident_id)
            alerts_futures.append(future)

        for alerts_future in futures.as_completed(alerts_futures):
            index = alerts_futures.index(alerts_future)
            incident_id = incidents[index]
            collected_alerts = alerts_future.result()
            collected_alerts.sort(key=lambda item: (item.created, -item.total_seconds()))

            # Simplify alerts - merge overlapping alerts for one incident.
            # If all alerts overlap, then simplify the id.
            merged_collected_alerts = merge_overlapping_alerts(collected_alerts)
            if len(merged_collected_alerts) == 1:
                merged_collected_alerts[0].ids = [incident_id]
            simplified_alerts.extend(merged_collected_alerts)

            # Keep also "original" alerts - just for logs and debugging.
            original_alerts.extend(collected_alerts)

    simplified_alerts.sort(key=lambda item: (item.created, -item.total_seconds()))
    merged_alerts = merge_overlapping_alerts(simplified_alerts)

    logging.info("len(incidents)={}".format(len(incidents)))
    logging.debug("incidents={}".format(str(incidents)))
    logging.info("len(original_alerts)={}".format(len(original_alerts)))
    logging.info("len(simplified_alerts)={}".format(len(simplified_alerts)))
    logging.info("len(merged_alerts)={}".format(len(merged_alerts)))
    logging.debug("merged_alerts={}".format(str(merged_alerts)))

    for interval_since, interval_until in intervals_gen(args.incidents_since, args.incidents_until, args.report_step):
        interval_alerts = filter_alerts(interval_since, interval_until, merged_alerts)
        report_uptime(interval_since, interval_until, interval_alerts, args.report_details_level)

    return 0


if __name__ == "__main__":
    sys.exit(main())
