#!venv/bin/python3

from __future__ import annotations

import argparse
import calendar
import concurrent
import concurrent.futures
import concurrent.futures.thread
import dataclasses
import datetime
import functools
import gzip
import itertools
import logging
import os
import pathlib
import pickle
import re
import sys
import time
import typing

import requests

VERSION = "2025-02-21"


class Cache:
    def __init__(self, directory: str = ".cache") -> None:
        self.cache_dir: pathlib.Path

        if pathlib.Path(directory).is_absolute():
            self.cache_dir = pathlib.Path(directory)
        else:
            self.cache_dir = pathlib.Path(os.path.realpath(__file__)).parent / directory

    def set(self, key: str, value: object) -> None:
        cache_file = self.cache_dir / f"{key}.pkl.gz"
        with gzip.open(cache_file, "wb", compresslevel=6) as f:
            # https://stackoverflow.com/q/79049420
            # noinspection PyTypeChecker
            pickle.dump(value, f)

    def get[T](self, key: str, _expected_type: type[T]) -> T:
        cache_file = self.cache_dir / f"{key}.pkl.gz"
        with gzip.open(cache_file, "rb") as f:
            return typing.cast(T, pickle.load(f))

    def __contains__(self, key: str) -> bool:
        return (self.cache_dir / f"{key}.pkl.gz").exists()


# date.timedelta, but with support for months and years.
@dataclasses.dataclass(frozen=True)
class TimeDelta:
    years: int = 0
    months: int = 0
    weeks: int = 0
    days: int = 0
    hours: int = 0
    minutes: int = 0
    seconds: int = 0

    # Support [TimeDelta(months=<n>) * int(<k>)] math, useful in loops.
    def __mul__(self, other: int) -> TimeDelta:
        return TimeDelta(
            years=self.years * other,
            months=self.months * other,
            weeks=self.weeks * other,
            days=self.days * other,
            hours=self.hours * other,
            minutes=self.minutes * other,
            seconds=self.seconds * other,
        )

    def __rmul__(self, other: int) -> TimeDelta:
        return self * other

    # Support [TimeDelta + TimeDelta] math.
    @typing.overload
    def __add__(self, other: TimeDelta) -> TimeDelta: ...

    # Support [TimeDelta + datetime.datetime] math.
    @typing.overload
    def __add__(self, other: datetime.datetime) -> datetime.datetime: ...

    def __add__(self, other: TimeDelta | datetime.datetime) -> TimeDelta | datetime.datetime:
        if isinstance(other, TimeDelta):
            return TimeDelta(
                years=self.years + other.years,
                months=self.months + other.months,
                weeks=self.weeks + other.weeks,
                days=self.days + other.days,
                hours=self.hours + other.hours,
                minutes=self.minutes + other.minutes,
                seconds=self.seconds + other.seconds,
            )
        if isinstance(other, datetime.datetime):
            # Add years and months.
            new_year = other.year + self.years + (other.month + self.months - 1) // 12
            new_month = (other.month + self.months - 1) % 12 + 1

            other_month_last_day = calendar.monthrange(other.year, other.month)[1]
            new_month_last_day = calendar.monthrange(new_year, new_month)[1]
            if other.day == other_month_last_day:
                new_day = new_month_last_day
            else:
                new_day = min(other.day, new_month_last_day)

            return (
                # New date, with years and months added.
                datetime.datetime(
                    year=new_year,
                    month=new_month,
                    day=new_day,
                    hour=other.hour,
                    minute=other.minute,
                    second=other.second,
                    tzinfo=other.tzinfo,
                )
                # add other TimeDelta stuff.
                + datetime.timedelta(
                    seconds=self.seconds,
                    minutes=self.minutes,
                    hours=self.hours,
                    days=self.days,
                    weeks=self.weeks,
                )
            )
        return NotImplemented

    def __radd__(self, other: datetime.datetime) -> datetime.datetime:
        return self + other


def parse_date(string: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(string)


class Incident:
    def __init__(self, id_: str | int, title: str, priority: str | None) -> None:
        self.id: str = str(id_)
        self.title: str = title
        self.priority: str | None = priority

    def __str__(self) -> str:
        return f"({self.id},{self.title},{self.priority})"

    def __repr__(self) -> str:
        return self.__str__()


class Alert:
    def __init__(
        self, ids: typing.Sequence[str | int], created: datetime.datetime, resolved: datetime.datetime
    ) -> None:
        self.ids: list[str] = [str(id_) for id_ in ids]
        self.created: datetime.datetime = created
        self.resolved: datetime.datetime = resolved

    def total_seconds(self) -> float:
        return (self.resolved - self.created).total_seconds()

    def __str__(self) -> str:
        return f"({self.ids},{self.created.isoformat()},{self.resolved.isoformat()})"

    def __repr__(self) -> str:
        return self.__str__()

    def __hash__(self) -> int:
        return hash((tuple(self.ids), self.created, self.resolved))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Alert):
            return self.ids == other.ids and self.created == other.created and self.resolved == other.resolved
        return False


# Call PageDuty API.
# Handles repeating errors and pagination.
# Extract all items for a given collector_key.
def call_pagerduty_api(
    call_id: str,
    session: requests.Session,
    api_token: str,
    url: str,
    params: dict[str, typing.Any],
    collector_key: str,
    offset: int = 0,
    retry: int = 0,
) -> list[dict[str, typing.Any]]:
    logging.info("call_pagerduty_api(%s, %s, %s)", call_id, offset, retry)

    params_with_pagination = {**params, "limit": 100, "offset": offset}
    headers = {"Accept": "application/vnd.pagerduty+json;version=2", "Authorization": f"Token token={api_token}"}

    response = session.get(url, params=params_with_pagination, headers=headers)

    if response.status_code != 200:
        if retry < 3:
            logging.info("response.status_code is %s, != 200, repeating", response.status_code)
            # https://v2.developer.pagerduty.com/docs/rate-limiting
            if response.status_code == 429:
                time.sleep(1)
            return call_pagerduty_api(call_id, session, api_token, url, params, collector_key, offset, retry + 1)
        msg = f"response.status_code is {response.status_code}, != 200"
        raise requests.HTTPError(msg)

    result = []

    api_response = response.json()
    result.extend(api_response[collector_key])

    # https://v2.developer.pagerduty.com/docs/pagination
    if api_response["more"]:
        next_offset = api_response["offset"] + len(api_response[collector_key])
        more_results = call_pagerduty_api(call_id, session, api_token, url, params, collector_key, next_offset, 0)
        result.extend(more_results)

    return result


# https://developer.pagerduty.com/api-reference/reference/REST/openapiv3.json/paths/~1incidents/get
def call_pagerduty_list_incidents(
    session: requests.Session,
    api_token: str,
    service_ids: list[str],
    start_date: datetime.datetime,
    end_date: datetime.datetime,
) -> list[Incident]:
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
        "incidents",
    )

    incidents = [
        Incident(
            api_incident["id"],
            api_incident["title"].replace("\t", " ").replace("\r", " ").replace("\n", " ").strip(),
            api_incident["priority"]["summary"] if api_incident.get("priority") else None,
        )
        for api_incident in api_incidents
    ]

    return incidents


# https://developer.pagerduty.com/api-reference/reference/REST/openapiv3.json/paths/~1incidents~1%7Bid%7D~1alerts/get
def call_pagerduty_list_alerts_for_an_incident(
    cache: Cache, session: requests.Session, api_token: str, incident_id: str
) -> list[Alert]:
    # Method result is considered stable and cached on disk.
    # Script process only resolved incidents.
    cache_item_id = f"alerts_for_an_incident-{incident_id}"

    if cache_item_id in cache:
        return cache.get(cache_item_id, list[Alert])

    api_alerts = call_pagerduty_api(
        f"https://api.pagerduty.com/incidents/{incident_id}/alerts",
        session,
        api_token,
        f"https://api.pagerduty.com/incidents/{incident_id}/alerts",
        {"statuses[]": "resolved", "time_zone": "UTC"},
        "alerts",
    )

    alerts = [
        Alert(
            [f"{incident_id}/{api_alert['id']}"],
            parse_date(api_alert["created_at"]),
            parse_date(api_alert["resolved_at"]),
        )
        for api_alert in api_alerts
    ]

    cache.set(cache_item_id, alerts)
    return alerts


# Find all alerts that overlap, and merge them.
# The resulting list includes only unique alerts that times do not overlap.
# Note: "alerts" array must be sorted by "created asc".
def merge_overlapping_alerts(alerts: list[Alert]) -> list[Alert]:
    ret = alerts[:]
    performed_any_merge = True
    while performed_any_merge:
        performed_any_merge = False
        for i in range(len(ret) - 1, 0, -1):
            if alerts_overlap(ret[i - 1], ret[i]):
                ret[i - 1] = merge_two_alerts(ret[i - 1], ret[i])
                ret.pop(i)
                performed_any_merge = True

    # Above algorithm should work. Let's check.
    for i, alert_1 in enumerate(ret):
        for j, alert_2 in enumerate(ret):
            if i != j and alerts_overlap(alert_1, alert_2):
                msg = f"Omg. {i} and {j} ({alert_1} and {alert_2}) overlaps! It's script bug."
                raise AssertionError(msg)
    return ret


# Function checks whether two alerts overlap.
# Math: https://nedbatchelder.com/blog/201310/range_overlap_in_two_compares.html
def alerts_overlap(alert_a: Alert, alert_b: Alert) -> bool:
    return alert_a.resolved >= alert_b.created and alert_b.resolved >= alert_a.created


# "Merge" means interval of new alert is union of intervals of two input *overlapping* alerts.
def merge_two_alerts(alert_a: Alert, alert_b: Alert) -> Alert:
    logging.debug("merge_two_alerts(%s,%s)", alert_a, alert_b)
    new_ids = alert_a.ids + alert_b.ids
    new_created = min(alert_a.created, alert_b.created)
    new_resolved = max(alert_a.resolved, alert_b.resolved)
    return Alert(new_ids, new_created, new_resolved)


# Filter alerts, return the ones in the interval [start_date, end_date).
# Note: Alert "created" date is compared.py
def filter_alerts(start_date: datetime.datetime, end_date: datetime.datetime, all_alerts: list[Alert]) -> list[Alert]:
    first_matching_index = next((i for i, v in enumerate(all_alerts) if v.created >= start_date), len(all_alerts))
    first_mismatched_index = next((i for i, v in enumerate(all_alerts) if v.created >= end_date), len(all_alerts))
    return all_alerts[first_matching_index:first_mismatched_index]


# Print the final report about uptime.
def report_uptime(
    start_date: datetime.datetime, end_date: datetime.datetime, interval_alerts: list[Alert], report_details_level: int
) -> None:
    interval_duration = (end_date - start_date).total_seconds()
    interval_downtime = sum(alert.total_seconds() for alert in interval_alerts)
    interval_uptime = (1 - (interval_downtime / interval_duration)) * 100
    interval_mttr = interval_downtime / len(interval_alerts) if interval_alerts else 0
    interval_ids = [alert.ids if len(alert.ids) > 1 else alert.ids[0] for alert in interval_alerts]
    end_date_inclusive = end_date - datetime.timedelta(seconds=1)

    report_formats = [
        "From: {} To: {} Uptime: {:6.2f} Incidents: {:3} Downtime: {: >8} Mttr: {: >8}",
        "From: {} To: {} Uptime: {:6.2f} Incidents: {:3} Downtime: {: >8} Mttr: {: >8} Incidents: {}",
    ]

    logging.warning(
        "%s",
        report_formats[report_details_level].format(
            start_date.isoformat(),
            end_date_inclusive.isoformat(),
            interval_uptime,
            len(interval_alerts),
            str(datetime.timedelta(seconds=interval_downtime)),
            str(datetime.timedelta(seconds=int(interval_mttr))),
            interval_ids,
        ),
    )


# Function that checks if an incident indicates a service outage.
# Incidents generated by StatusCake start with "Website | Your site".
def is_outage(
    title_checks: list[re.Pattern[str]] | None,
    title: str,
    priority_checks: list[str] | None,
    priority: str | None,
) -> bool:
    if title_checks:
        if not any(title_check.search(title) for title_check in title_checks):
            return False
    if priority_checks:
        if not any(priority_check == priority for priority_check in priority_checks):
            return False
    return True


# Generate intervals from start_date (inclusive) to end_date (exclusive) with time_delta step.
# Example: start_date=2019-01-01, end_date=2020-01-01,
# time_delta=6months will return two intervals:
#          [2019-01-01 00:00:00, 2019-07-01 00:00:00)
#          [2019-07-01 00:00:00, 2020-01-01 00:00:00)
def intervals_gen(
    start_date: datetime.datetime, end_date: datetime.datetime, time_delta: TimeDelta
) -> typing.Iterator[tuple[datetime.datetime, datetime.datetime]]:
    # This could be simpler, but then the "end of month" case would be broken.
    # Please see test TestIntervalsGen.test7.
    for n in itertools.count():
        interval_since = start_date + (time_delta * n)
        interval_until = min(start_date + (time_delta * (n + 1)), end_date)
        yield interval_since, interval_until

        if interval_until >= end_date:
            break


def parse_service_id(string: str) -> str:
    match = re.search(r"https://[a-zA-Z0-9.-_]*pagerduty\.com/(?:services|service-directory)/([a-zA-Z0-9]+)", string)
    return match.group(1) if match else string


def parse_time_delta(string: str) -> TimeDelta:
    match = re.match(r"^(\d+) *(hour|day|month|year)s?$", string)
    if match is None:
        msg = f"Invalid time data string: {string}."
        raise ValueError(msg)
    num = int(match.group(1))
    unit = match.group(2)
    return {
        "hour": TimeDelta(hours=num),
        "day": TimeDelta(days=num),
        "month": TimeDelta(months=num),
        "year": TimeDelta(years=num),
    }[unit]


def main() -> int:
    # args
    argparser = argparse.ArgumentParser(description="pagerduty-service-uptime")
    argparser.add_argument(
        "--log-level",
        metavar="LOGLEVEL",
        dest="log_level",
        type=str,
        required=True,
        help="verbosity level, one of the following values: CRITICAL, ERROR, WARN, INFO, DEBUG, NOTSET",
    )
    argparser.add_argument(
        "--api-token",
        metavar="APITOKEN",
        dest="api_token",
        type=str,
        required=True,
        help="personal REST API Key for PagerDuty service "
        "(https://support.pagerduty.com/docs/generating-api-keys#section-generating-a-personal-rest-api-key)",
    )
    argparser.add_argument(
        "--service-ids",
        metavar="SERVICEID",
        dest="service_ids",
        type=parse_service_id,
        nargs="+",
        required=True,
        help="services for which the script will make calculations, "
        "values can be service ID (e.g., ABCDEF4) "
        "or service URL (e.g., https://some.pagerduty.com/service-directory/ABCDEF4)",
    )
    argparser.add_argument(
        "--title-checks",
        metavar="PATTERN",
        dest="title_checks",
        type=functools.partial(re.compile, flags=0),
        nargs="*",
        required=False,
        help="regular expressions for checking title, eg. '^Downtime' '^Outage'; "
        "the event title must match any of the given regular expressions to be considered downtime; "
        "if none is specified, the marking as downtime does not depend on the title",
    )
    argparser.add_argument(
        "--priority-checks",
        metavar="PRIORITY",
        dest="priority_checks",
        type=str,
        nargs="*",
        required=False,
        help="values for checking priority, eg. 'P1' 'P2'; "
        "the event priority must match any of the given values to be considered downtime; "
        "if none is specified, the marking as downtime does not depend on the priority",
    )
    argparser.add_argument(
        "--incidents-since",
        metavar="ISODATE",
        dest="incidents_since",
        type=parse_date,
        required=True,
        help="start date of the time range to be checked (inclusive); "
        "must be in iso8601 format, e.g., '2019-01-01T00:00:00Z'",
    )
    argparser.add_argument(
        "--incidents-until",
        metavar="ISODATE",
        dest="incidents_until",
        type=parse_date,
        required=True,
        help="end date of the time range to be checked (exclusive); "
        "must be in iso8601 format, e.g., '2020-01-01T00:00:00Z'",
    )
    argparser.add_argument(
        "--report-step",
        metavar="STEP",
        dest="report_step",
        type=parse_time_delta,
        required=True,
        help="report step, e.g., '14 days', '6 months', '1 year'",
    )
    argparser.add_argument(
        "--report-details-level",
        metavar="LVL",
        dest="report_details_level",
        type=int,
        choices=[0, 1],
        required=False,
        default=0,
        help="detail level of the report, one of the following values: 0, 1; higher value = more details",
    )
    argparser.add_argument("--version", action="version", version=("%(prog)s " + VERSION))
    args = argparser.parse_args()

    # logging
    logging.basicConfig(stream=sys.stdout, level=args.log_level, format="%(levelname)s %(message)s")
    logging.info("log_level=%s", logging.getLevelName(args.log_level))
    logging.info("api_token=%s...", args.api_token[:3])
    logging.info("service_ids=%s", args.service_ids)
    logging.info("title_checks=%s", args.title_checks)
    logging.info("priority_checks=%s", args.priority_checks)
    logging.info("incidents_since=%s", args.incidents_since)
    logging.info("incidents_until=%s", args.incidents_until)
    logging.info("report_step=%s", args.report_step)

    # collect incidents
    incidents: list[str] = []
    incidents_filtered_out: list[str] = []
    with requests.Session() as requests_session:
        collect_step = TimeDelta(months=4)
        for interval_since, interval_until in intervals_gen(args.incidents_since, args.incidents_until, collect_step):
            collected_incidents = call_pagerduty_list_incidents(
                requests_session,
                args.api_token,
                args.service_ids,
                interval_since,
                interval_until,
            )

            # Filter incidents - keep only those that indicate a service outage.
            for incident in collected_incidents:
                if is_outage(args.title_checks, incident.title, args.priority_checks, incident.priority):
                    incidents.append(incident.id)
                else:
                    incidents_filtered_out.append(incident.id)

    # collect alerts
    cache = Cache()
    alerts_futures: list[concurrent.futures.Future[list[Alert]]] = []
    original_alerts: list[Alert] = []
    simplified_alerts: list[Alert] = []
    merged_alerts: list[Alert]
    with (
        concurrent.futures.thread.ThreadPoolExecutor(max_workers=8) as executor,
        requests.Session() as requests_session,
    ):
        for incident_id in incidents:
            future = executor.submit(
                call_pagerduty_list_alerts_for_an_incident, cache, requests_session, args.api_token, incident_id
            )
            alerts_futures.append(future)

        for alerts_future in concurrent.futures.as_completed(alerts_futures):
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

    logging.info("len(incidents)=%s", len(incidents))
    logging.info("len(incidents_filtered_out)=%s", len(incidents_filtered_out))
    logging.debug("incidents=%s", incidents)
    logging.debug("incidents_filtered_out=%s", incidents_filtered_out)
    logging.info("len(original_alerts)=%s", len(original_alerts))
    logging.info("len(simplified_alerts)=%s", len(simplified_alerts))
    logging.info("len(merged_alerts)=%s", len(merged_alerts))
    logging.debug("merged_alerts=%s", merged_alerts)

    for interval_since, interval_until in intervals_gen(args.incidents_since, args.incidents_until, args.report_step):
        interval_alerts = filter_alerts(interval_since, interval_until, merged_alerts)
        report_uptime(interval_since, interval_until, interval_alerts, args.report_details_level)

    return 0


if __name__ == "__main__":
    sys.exit(main())
