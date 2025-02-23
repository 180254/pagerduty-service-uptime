#!venv/bin/python3

from __future__ import annotations

import argparse
import calendar
import concurrent
import concurrent.futures
import concurrent.futures.thread
import csv
import dataclasses
import datetime
import io
import itertools
import json
import logging
import os
import pathlib
import re
import shelve
import sys
import threading
import time
import typing

import requests

if typing.TYPE_CHECKING:
    import types

VERSION = "2025-02-28"


class Cache:
    def __init__(self, cache_location: str = ".cache/cache") -> None:
        self.cache_location: str = cache_location
        self.cache_path: pathlib.Path = (
            pathlib.Path(cache_location)
            if pathlib.Path(cache_location).is_absolute()
            else pathlib.Path(os.path.realpath(__file__)).parent / cache_location
        )

        self.cache: shelve.Shelf[object] | None = None
        self.rlock: threading.RLock = threading.RLock()

    def __enter__(self) -> typing.Self:
        with self.rlock:
            if self.cache is not None:
                msg = f"Cache already opened: {self.cache_location}."
                raise RuntimeError(msg)

            self.cache = shelve.open(self.cache_path)
        return self

    def __exit__(
        self,
        type0: type[BaseException] | None,
        value: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> None:
        with self.rlock:
            if type0 and value:
                logging.error("Exception in Cache.__exit__", exc_info=(type0, value, traceback))
            if self.cache is None:
                msg = f"Cache already closed: {self.cache_location}."
                raise RuntimeError(msg)

            self.cache.close()
            self.cache = None

    def set(self, key: str, value: object) -> None:
        with self.rlock:
            if self.cache is None:
                msg = f"Cache not opened: {self.cache_location}."
                raise RuntimeError(msg)

            self.cache[key] = value

    def get[T](self, key: str, _expected_type: type[T]) -> T:
        with self.rlock:
            if self.cache is None:
                msg = f"Cache not opened: {self.cache_location}."
                raise RuntimeError(msg)

            return typing.cast(T, self.cache[key])

    def __contains__(self, key: str) -> bool:
        with self.rlock:
            if self.cache is None:
                msg = f"Cache not opened: {self.cache_location}."
                raise RuntimeError(msg)

            return key in self.cache


# Filter list[data], based on specified criteria.
#   Filter syntax:
#     path:operator:values
#     not(path:operator:values)
#   Attributes:
#     path      - A dot-separated identifier in the item structure
#     operator  - The action to perform, specifically 'matches'.
#     values    - A list of comma-separated patterns for matching.
# Examples:
#     priority.summary:matches:P1,P2
#     integration.summary:matches:StatusCake,AlertSite,Grafana
#     not(title:matches:Test alert,testsite,Test notification)
#     integration.summary:matches
#     obj.matches:expectedValue
#     obj.array:matches:expectedValue
#     obj.array[0].matches:expectedValue
@dataclasses.dataclass(frozen=True)
class Filter:
    negation: bool
    path: list[str]
    operator: typing.Literal["matches"]
    values: list[str]

    def __str__(self) -> str:
        result = f"{'.'.join(self.path)}:{self.operator}:{','.join(self.values)}"
        if self.negation:
            result = f"not({result})"
        return result

    def __repr__(self) -> str:
        return f"Filter(not={self.negation}, path={self.path}, operator={self.operator}, values={self.values})"

    @classmethod
    def parse(cls, filter_str: str) -> Filter:
        negation = filter_str.startswith("not(") and filter_str.endswith(")")
        if negation:
            filter_str = filter_str[4:-1]

        try:
            path_str, operator_str, values_str = filter_str.split(":", 2)
        except ValueError:
            path_str, operator_str = filter_str.split(":")
            values_str = ""

        if operator_str != "matches":
            msg = f"Invalid filter string: {filter_str}"
            raise ValueError(msg)

        path = path_str.split(".") if path_str not in {"", "."} else []
        operator = typing.cast(typing.Literal["matches"], operator_str)
        values = next((csv.reader(io.StringIO(values_str), dialect="unix")), [])

        return cls(negation, path, operator, values)

    def check(self, check_key: str, data: dict[str, typing.Any]) -> bool:
        check_value = self._get_value(data)
        if isinstance(check_value, list | dict | bool):
            check_value_str = json.dumps(check_value, separators=(",", ":"))
        else:
            check_value_str = str(check_value)

        if self.values:
            result = any(re.search(v, check_value_str) for v in self.values)
        else:
            result = bool(check_value)

        if self.negation:
            result = not result

        if not result:
            logging.info(
                "Filter.check(%s)=%s for id=%s, check_key=%s, check_value_str=%s",
                self,
                data.get("id"),
                result,
                check_key,
                check_value_str,
            )

        return result

    # Retrieve the value from the nested dictionary based on the specified path.
    # Returns None if the path does not exist.
    def _get_value(self, data: dict[str, typing.Any]) -> typing.Any:
        value: typing.Any = data
        for key in self.path:
            if isinstance(value, dict):
                value = value.get(key)
            elif isinstance(value, list) and key.isdigit() and len(value) > int(key):
                value = value[int(key)]
            else:
                return None
        return value


# Extends datetime.timedelta to support months and years.
@dataclasses.dataclass(frozen=True)
class TimeDelta:
    years: int = 0
    months: int = 0
    weeks: int = 0
    days: int = 0
    hours: int = 0
    minutes: int = 0
    seconds: int = 0

    def __str__(self) -> str:
        parts = [f"{field}={value}" for field, value in vars(self).items() if value]
        return f"{', '.join(parts)}"

    def __repr__(self) -> str:
        return f"TimeDelta({self})"

    # Support [TimeDelta(months=<n>) * int(<k>)] math, useful in loops.
    def __mul__(self, other: int) -> TimeDelta:
        return TimeDelta(**{field: value * other for field, value in vars(self).items()})

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
                **{field: (getattr(self, field) + getattr(other, field)) for field, value in vars(self).items()}
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
                # Create a new date by adding the specified number of years and months.
                datetime.datetime(
                    year=new_year,
                    month=new_month,
                    day=new_day,
                    hour=other.hour,
                    minute=other.minute,
                    second=other.second,
                    tzinfo=other.tzinfo,
                )
                # Add other TimeDelta components to the new date.
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


def parse_date(date_str: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(date_str)


def parse_time_delta(delta_str: str) -> TimeDelta:
    match = re.match(r"^(\d+) *(hour|day|month|year)s?$", delta_str)
    if not match:
        msg = f"Invalid time data string: {delta_str}."
        raise ValueError(msg)
    num, unit = int(match.group(1)), match.group(2)
    return TimeDelta(**{f"{unit}s": num})


def extract_pagerduty_service_id(value: str) -> str:
    match = re.search(r"https://[a-zA-Z0-9.-_]*pagerduty\.com/(?:services|service-directory)/([a-zA-Z0-9]+)", value)
    if match:
        return match.group(1)
    return value


@dataclasses.dataclass(frozen=True)
class Incident:
    id: str
    raw_data: dict[str, typing.Any] | None = None

    def __str__(self) -> str:
        return f"({self.id})"

    def __repr__(self) -> str:
        return f"Incident{self}"


@dataclasses.dataclass(frozen=True)
class Alert:
    ids: list[str | int]
    created: datetime.datetime
    resolved: datetime.datetime
    raw_data: dict[str, typing.Any] | None = None

    def total_seconds(self) -> float:
        return (self.resolved - self.created).total_seconds()

    def __str__(self) -> str:
        return f"({self.ids},{self.created.isoformat()},{self.resolved.isoformat()})"

    def __repr__(self) -> str:
        return f"Alert{self}"


# Function to call the PagerDuty API.
# Handles repeating errors and pagination.
# Extract all items for a given collector_key.
def call_pagerduty_api(
    call_id: str,
    session: requests.Session,
    api_token: str,
    url: str,
    params: dict[str, typing.Any],
    collector_key: str,
) -> list[dict[str, typing.Any]]:
    # https://developer.pagerduty.com/docs/authentication
    # https://developer.pagerduty.com/docs/versioning
    headers = {"Accept": "application/vnd.pagerduty+json;version=2", "Authorization": f"Token token={api_token}"}

    results = []
    offset = 0
    retry, max_retries = 0, 3
    while True:
        logging.info("call_pagerduty_api(call_id=%s, offset=%s, retry=%s)", call_id, offset, retry)

        params_with_pagination = {**params, "limit": 100, "offset": offset}
        response: requests.Response = session.get(url, params=params_with_pagination, headers=headers)

        if response.status_code != requests.codes.ok:
            if retry < max_retries:
                logging.info("response.status_code is %s, != 200, repeating", response.status_code)
                # https://developer.pagerduty.com/docs/rest-api-rate-limits
                if response.status_code == requests.codes.too_many_requests:
                    logging.info(
                        "Too Many Requests: limit: %s, remaining: %s, reset: %s",
                        response.headers["ratelimit-limit"],
                        response.headers["ratelimit-remaining"],
                        response.headers["ratelimit-reset"],
                    )
                    time.sleep(float(response.headers["ratelimit-reset"]))
                retry += 1
                continue
            msg = f"response.status_code is {response.status_code}, != 200"
            raise requests.HTTPError(msg)

        api_response = response.json()
        results.extend(api_response[collector_key])

        # https://v2.developer.pagerduty.com/docs/pagination
        if not api_response.get("more", False):
            break

        offset = api_response["offset"] + len(api_response[collector_key])
        retry = 0

    return results


# https://developer.pagerduty.com/api-reference/reference/REST/openapiv3.json/paths/~1incidents/get
def call_pagerduty_list_incidents(
    session: requests.Session,
    api_token: str,
    service_ids: list[str],
    start_date: datetime.datetime,
    end_date: datetime.datetime,
) -> list[Incident]:
    params = {
        "since": start_date.isoformat(),
        "until": end_date.isoformat(),  # until is exclusive
        "service_ids[]": service_ids,
        "statuses[]": "resolved",
        "time_zone": "UTC",
        "sort_by": "created_at",
    }
    api_incidents = call_pagerduty_api(
        f"https://api.pagerduty.com/incidents,since={start_date.isoformat()},until={end_date.isoformat()}",
        session,
        api_token,
        "https://api.pagerduty.com/incidents",
        params,
        "incidents",
    )
    return [
        Incident(
            api_incident["id"],
            api_incident,
        )
        for api_incident in api_incidents
    ]


# https://developer.pagerduty.com/api-reference/reference/REST/openapiv3.json/paths/~1incidents~1%7Bid%7D~1alerts/get
def call_pagerduty_list_alerts_for_an_incident(
    cache: Cache, session: requests.Session, api_token: str, incident_id: str
) -> list[Alert]:
    # Method result is considered stable and cached on disk.
    # Script process only resolved incidents.
    cache_key = f"alerts_for_an_incident-{incident_id}"

    if cache_key in cache:
        return cache.get(cache_key, list[Alert])

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
            api_alert,
        )
        for api_alert in api_alerts
    ]

    cache.set(cache_key, alerts)
    return alerts


# Identify and merge any overlapping alerts.
# The resulting list will contain only unique alerts with non-overlapping times.
# Note: The "alerts" list must be sorted by the "created" timestamp in ascending order.
def merge_overlapping_alerts(alerts: list[Alert]) -> list[Alert]:
    merged_alerts: list[Alert] = []
    for alert in alerts:
        if merged_alerts and alerts_overlap(merged_alerts[-1], alert):
            merged_alerts[-1] = merge_two_alerts(merged_alerts[-1], alert)
        else:
            merged_alerts.append(alert)

    if "unittest" in sys.modules:
        # Above algorithm should work. Let's check.
        for i, alert_1 in enumerate(merged_alerts):
            for j, alert_2 in enumerate(merged_alerts):
                if i != j and alerts_overlap(alert_1, alert_2):
                    msg = f"Omg. {i} and {j} ({alert_1} and {alert_2}) overlaps! It's script bug."
                    raise AssertionError(msg)

    return merged_alerts


# This function determines if two alerts overlap.
# For more information on the mathematical approach used, refer to:
# https://nedbatchelder.com/blog/201310/range_overlap_in_two_compares.html
def alerts_overlap(alert_a: Alert, alert_b: Alert) -> bool:
    return alert_a.resolved >= alert_b.created and alert_b.resolved >= alert_a.created


# "Merge" means the interval of the new alert is the union of the intervals
# (of the two input overlapping alerts).
def merge_two_alerts(alert_a: Alert, alert_b: Alert) -> Alert:
    logging.debug("merge_two_alerts(%s,%s)", alert_a, alert_b)
    return Alert(
        ids=alert_a.ids + alert_b.ids,
        created=min(alert_a.created, alert_b.created),
        resolved=max(alert_a.resolved, alert_b.resolved),
    )


# Filters the list of alerts and returns those that fall within the specified interval [start_date, end_date).
# Note: The comparison is based on the "created" timestamp of the alerts.
def filter_alerts(start_date: datetime.datetime, end_date: datetime.datetime, all_alerts: list[Alert]) -> list[Alert]:
    return [alert for alert in all_alerts if start_date <= alert.created < end_date]


# Function to determine if an incident/alert qualifies as a service outage.
def is_outage(
    check_key: str,
    filters: list[Filter],
    data: dict[str, typing.Any],
) -> bool:
    return all(filter0.check(check_key, data) for filter0 in filters)


# Generate intervals from the start_date (inclusive) to the end_date (exclusive) using the specified time_delta step.
# Example:
#   start_date=2019-01-01 end_date=2020-01-01 time_delta=6 months
# will generate the following intervals:
#          [2019-01-01 00:00:00, 2019-07-01 00:00:00)
#          [2019-07-01 00:00:00, 2020-01-01 00:00:00)
def intervals_gen(
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    time_delta: TimeDelta,
) -> typing.Iterator[tuple[datetime.datetime, datetime.datetime]]:
    # This could be simpler, but then the "end of month" case would be broken.
    # Please see tests: TestIntervalsGen.test7, TestIntervalsGen.test8.
    for n in itertools.count():
        interval_since = start_date + (time_delta * n)
        interval_until = min(start_date + (time_delta * (n + 1)), end_date)
        yield interval_since, interval_until

        if interval_until >= end_date:
            break


@dataclasses.dataclass(frozen=True)
class Args:
    log_level: str
    api_token: str
    service_ids: list[str]
    incident_filters: list[Filter]
    alert_filters: list[Filter]
    incidents_since: datetime.datetime
    incidents_until: datetime.datetime
    report_step: TimeDelta
    report_details_level: int


# Fetch incidents from the PagerDuty service using the provided args.
# :return: tuple[collected, spurned]
def collect_incidents(
    args: Args,
    session: requests.Session,
) -> tuple[list[Incident], list[Incident]]:
    collected: list[Incident] = []
    spurned: list[Incident] = []

    collect_step = TimeDelta(months=4)
    for interval_since, interval_until in intervals_gen(args.incidents_since, args.incidents_until, collect_step):
        pagerduty_incidents = call_pagerduty_list_incidents(
            session,
            args.api_token,
            args.service_ids,
            interval_since,
            interval_until,
        )

        for incident in pagerduty_incidents:
            if incident.raw_data and is_outage("incidents", args.incident_filters, incident.raw_data):
                collected.append(incident)
            else:
                spurned.append(incident)

    return collected, spurned


# Fetch alerts from the PagerDuty service using the provided incidents.
# :return: tuple[collected, spurned, simplified, merged]
def collect_and_merge_alerts(
    args: Args,
    session: requests.Session,
    cache: Cache,
    incidents: list[Incident],
) -> tuple[list[Alert], list[Alert], list[Alert], list[Alert]]:
    collected: list[Alert] = []
    spurned: list[Alert] = []
    simplified: list[Alert] = []

    def alert_sort_key(alert0: Alert) -> tuple[datetime.datetime, float]:
        return alert0.created, -alert0.total_seconds()

    max_workers = min(8, os.cpu_count() or 1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_incident = {
            executor.submit(
                call_pagerduty_list_alerts_for_an_incident, cache, session, args.api_token, incident.id
            ): incident
            for incident in incidents
        }

    for future in concurrent.futures.as_completed(future_to_incident):
        incident = future_to_incident[future]
        alerts_for_incident = future.result()
        alerts_for_incident.sort(key=alert_sort_key)

        collected_for_incident: list[Alert] = []
        spurned_for_incident: list[Alert] = []
        for alert in alerts_for_incident:
            if alert.raw_data and is_outage("alerts", args.alert_filters, alert.raw_data):
                collected_for_incident.append(alert)
            else:
                spurned_for_incident.append(alert)

        merged_for_incident = merge_overlapping_alerts(collected_for_incident)
        if len(merged_for_incident) == 1 and not spurned_for_incident:
            merged_for_incident[0] = Alert(
                ids=[incident.id],
                created=merged_for_incident[0].created,
                resolved=merged_for_incident[0].resolved,
            )

        collected.extend(collected_for_incident)
        spurned.extend(spurned_for_incident)
        simplified.extend(merged_for_incident)

    collected.sort(key=alert_sort_key)
    spurned.sort(key=alert_sort_key)
    simplified.sort(key=alert_sort_key)

    merged = merge_overlapping_alerts(simplified)

    return collected, spurned, simplified, merged


# Generate and display the final uptime report.
def report_uptime(
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    alerts: list[Alert],
    report_details_level: int,
) -> None:
    duration = (end_date - start_date).total_seconds()
    downtime = sum(alert.total_seconds() for alert in alerts)
    uptime = (1 - (downtime / duration)) * 100 if duration else 100
    mttr = downtime / len(alerts) if alerts else 0
    ids = [alert.ids if len(alert.ids) > 1 else alert.ids[0] for alert in alerts]
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
            uptime,
            len(alerts),
            str(datetime.timedelta(seconds=downtime)),
            str(datetime.timedelta(seconds=int(mttr))),
            ids,
        ),
    )


# https://stackoverflow.com/a/45392259
# usage: argument_parser.add_argument(..., **environ_or_required("ENV_VAR")))
def environ_or_required(key: str) -> dict[str, typing.Any]:
    value = os.environ.get(key)
    if value:
        return {"default": value}
    return {"required": True}


def main() -> int:
    # args
    arg_parser = argparse.ArgumentParser(
        description="pagerduty-service-uptime", formatter_class=argparse.RawTextHelpFormatter
    )
    arg_parser.add_argument(
        "--log-level",
        metavar="LOG_LEVEL",
        dest="log_level",
        type=str,
        choices=["CRITICAL", "ERROR", "WARN", "INFO", "DEBUG", "NOTSET"],
        required=True,
        help="Set the verbosity level. Choose from: CRITICAL, ERROR, WARN, INFO, DEBUG, NOTSET.",
    )
    arg_parser.add_argument(
        "--api-token",
        metavar="API_TOKEN",
        dest="api_token",
        type=str,
        **environ_or_required("PAGERDUTY_TOKEN"),
        help="Personal REST API Key for the PagerDuty service.",
    )
    arg_parser.add_argument(
        "--service-ids",
        metavar="SERVICE_ID",
        dest="service_ids",
        type=extract_pagerduty_service_id,
        nargs="+",
        required=True,
        help=(
            "Services for which the script will perform calculations.\n"
            "Values can be service ID (e.g., ABCDEF4) or\n"
            "service URL (e.g., https://some.pagerduty.com/service-directory/ABCDEF4)."
        ),
    )
    filters_doc = (
        "Filter Syntax: path:operator:patterns, not(path:operator:patterns)\n"
        "  - path: A dot-separated identifier in the item structure.\n"
        "  - operator: The action to perform, specifically 'matches'.\n"
        "  - patterns: A list of comma-separated patterns for matching.\n"
        "To qualify an item as downtime:\n"
        "  - All specified filters must match the item (AND condition).\n"
        "  - For each filter, at least one pattern must match the item (OR condition).\n"
        "If no filters are provided, all items are treated as downtime.\n"
        "Examples:\n"
        "  - 'integration.summary:matches:StatusCake,AlertSite'\n"
        "  - 'integration.summary:matches'\n"
        "  - 'not(integration.summary:matches)'\n"
        "Special Cases:\n"
        "  - The expression 'path:matches' indicates that the path exists and has a non-false-like value.\n"
        "    False-like values include: '', '0', 'false', 'null', '[]', '{}'.\n"
        "Paths referencing objects will be converted into a compact JSON format for pattern validation."
    )
    arg_parser.add_argument(
        "--incident-filters",
        metavar="FILTER",
        dest="incident_filters",
        type=Filter.parse,
        nargs="*",
        required=False,
        default=[],
        help=(
            "Add incidents filter.\n"
            f"{filters_doc}\n"
            "Incident structure is described at: https://developer.pagerduty.com/api-reference/9d0b4b12e36f9-list-incidents"
        ),
    )
    arg_parser.add_argument(
        "--alert-filters",
        metavar="FILTER",
        dest="alert_filters",
        type=Filter.parse,
        nargs="*",
        required=False,
        default=[],
        help=(
            "Add alerts filter.\n"
            f"{filters_doc}\n"
            "Alert structure is described at: https://developer.pagerduty.com/api-reference/4bc42e7ac0c59-list-alerts-for-an-incident"
        ),
    )
    arg_parser.add_argument(
        "--incidents-since",
        metavar="ISO_DATE",
        dest="incidents_since",
        type=parse_date,
        required=True,
        help=(
            "Start date of the time range to be checked (inclusive).\n"
            "Must be ISO 8601 formatted, e.g., '2019-01-01T00:00:00Z'."
        ),
    )
    arg_parser.add_argument(
        "--incidents-until",
        metavar="ISO_DATE",
        dest="incidents_until",
        type=parse_date,
        required=True,
        help=(
            "End date of the time range to be checked (exclusive).\n"
            "Must be ISO 8601 formatted, e.g., '2019-01-01T00:00:00Z'."
        ),
    )
    arg_parser.add_argument(
        "--report-step",
        metavar="TIME_DELTA",
        dest="report_step",
        type=parse_time_delta,
        required=True,
        help="Step interval for the report, e.g., '14 days', '6 months', '1 year'.",
    )
    arg_parser.add_argument(
        "--report-details-level",
        metavar="REPORT_LEVEL",
        dest="report_details_level",
        type=int,
        choices=[0, 1],
        required=False,
        default=0,
        help=(
            "Specify the level of detail for the report.\n"
            "Acceptable values are: 0, 1. Higher values provide more detailed information."
        ),
    )
    arg_parser.add_argument("--version", action="version", version=("%(prog)s " + VERSION))
    args = Args(**vars(arg_parser.parse_args()))

    logging.basicConfig(stream=sys.stdout, level=args.log_level, format="%(levelname)s %(message)s")
    logging.info("log_level=%s", args.log_level)
    logging.info("api_token=%s...%s", args.api_token[:3], args.api_token[-3:])
    logging.info("service_ids=%s", args.service_ids)
    logging.info("incident_filters=%s", args.incident_filters)
    logging.info("alert_filters=%s", args.alert_filters)
    logging.info("incidents_since=%s", args.incidents_since)
    logging.info("incidents_until=%s", args.incidents_until)
    logging.info("report_step=%r", args.report_step)
    logging.info("report_details_level=%s", args.report_details_level)

    with requests.Session() as session, Cache() as cache:
        incidents_collected, incidents_spurned = collect_incidents(args, session)
        alerts_collected, alerts_spurned, alerts_simplified, alerts_merged = collect_and_merge_alerts(
            args,
            session,
            cache,
            incidents_collected,
        )

    logging.info("len(incidents_collected)=%d", len(incidents_collected))
    logging.info("len(incidents_spurned)=%d", len(incidents_spurned))
    logging.info("len(alerts_collected)=%d", len(alerts_collected))
    logging.info("len(alerts_spurned)=%d", len(alerts_spurned))
    logging.info("len(alerts_simplified)=%d", len(alerts_simplified))
    logging.info("len(alerts_merged)=%d", len(alerts_merged))

    logging.debug("incidents_collected=%s", incidents_collected)
    logging.debug("incidents_spurned=%s", incidents_spurned)
    logging.debug("alerts_spurned=%s", alerts_spurned)
    logging.debug("alerts_merged=%s", alerts_merged)

    for interval_since, interval_until in intervals_gen(args.incidents_since, args.incidents_until, args.report_step):
        interval_alerts = filter_alerts(interval_since, interval_until, alerts_merged)
        report_uptime(interval_since, interval_until, interval_alerts, args.report_details_level)

    return 0


if __name__ == "__main__":
    sys.exit(main())
