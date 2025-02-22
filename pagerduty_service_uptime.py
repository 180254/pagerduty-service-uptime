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
import itertools
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

VERSION = "2025-02-22"


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
        type_: type[BaseException] | None,
        value: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> None:
        with self.rlock:
            if type_ and value:
                logging.error("Exception in Cache.__exit__", exc_info=(type_, value, traceback))
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

    def __str__(self) -> str:
        parts = [f"{field}={value}" for field, value in vars(self).items() if value]
        return f"TimeDelta({', '.join(parts)})"

    def __repr__(self) -> str:
        return str(self)

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


def parse_time_delta(string: str) -> TimeDelta:
    match = re.match(r"^(\d+) *(hour|day|month|year)s?$", string)
    if not match:
        msg = f"Invalid time data string: {string}."
        raise ValueError(msg)
    num = int(match.group(1))
    unit = match.group(2)
    return TimeDelta(**{unit + "s": num})


def parse_service_id(string: str) -> str:
    match = re.search(r"https://[a-zA-Z0-9.-_]*pagerduty\.com/(?:services|service-directory)/([a-zA-Z0-9]+)", string)
    return match.group(1) if match else string


@dataclasses.dataclass(frozen=True)
class Incident:
    id: str
    title: str
    priority: str | None


@dataclasses.dataclass(frozen=True)
class Alert:
    ids: list[str | int]
    created: datetime.datetime
    resolved: datetime.datetime

    def total_seconds(self) -> float:
        return (self.resolved - self.created).total_seconds()

    def __str__(self) -> str:
        return f"({self.ids},{self.created.isoformat()},{self.resolved.isoformat()})"

    def __repr__(self) -> str:
        return str(self)


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
) -> list[dict[str, typing.Any]]:
    # https://developer.pagerduty.com/docs/authentication
    # https://developer.pagerduty.com/docs/versioning
    headers = {"Accept": "application/vnd.pagerduty+json;version=2", "Authorization": f"Token token={api_token}"}

    result = []
    offset = 0
    retry = 0
    max_retries = 3
    while True:
        logging.info("call_pagerduty_api(call_id=%s, offset=%s, retry=%s)", call_id, offset, retry)

        params_with_pagination = {**params, "limit": 100, "offset": offset}
        response = session.get(url, params=params_with_pagination, headers=headers)

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
        result.extend(api_response[collector_key])

        # https://v2.developer.pagerduty.com/docs/pagination
        if not api_response["more"]:
            break

        offset = api_response["offset"] + len(api_response[collector_key])
        retry = 0

    return result


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
            api_incident["title"].replace("\t", " ").replace("\r", " ").replace("\n", " ").strip(),
            api_incident["priority"]["summary"] if api_incident.get("priority") else None,
        )
        for api_incident in api_incidents
    ]


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
    ret: list[Alert] = []
    for alert in alerts:
        if ret and alerts_overlap(ret[-1], alert):
            ret[-1] = merge_two_alerts(ret[-1], alert)
        else:
            ret.append(alert)

    if "unittest" in sys.modules:
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


# "Merge" means interval of new alert is union of intervals (of two input *overlapping* alerts).
def merge_two_alerts(alert_a: Alert, alert_b: Alert) -> Alert:
    logging.debug("merge_two_alerts(%s,%s)", alert_a, alert_b)
    return Alert(
        ids=alert_a.ids + alert_b.ids,
        created=min(alert_a.created, alert_b.created),
        resolved=max(alert_a.resolved, alert_b.resolved),
    )


# Filter alerts, return the ones in the interval [start_date, end_date).
# Note: Alert "created" date is compared.
def filter_alerts(start_date: datetime.datetime, end_date: datetime.datetime, all_alerts: list[Alert]) -> list[Alert]:
    return [alert for alert in all_alerts if start_date <= alert.created < end_date]


# Function that checks if an incident indicates a service outage.
# Incidents generated by StatusCake start with "Website | Your site".
def is_outage(
    title_checks: list[re.Pattern[str]] | None,
    title: str,
    priority_checks: list[str] | None,
    priority: str | None,
) -> bool:
    title_matches = not title_checks or any(check.search(title) for check in title_checks)
    priority_matches = not priority_checks or priority in priority_checks
    return title_matches and priority_matches


# Generate intervals from start_date (inclusive) to end_date (exclusive) with time_delta step.
# Example: start_date=2019-01-01, end_date=2020-01-01,
# time_delta=6months will return two intervals:
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
    title_checks: list[re.Pattern[str]]
    priority_checks: list[str]
    incidents_since: datetime.datetime
    incidents_until: datetime.datetime
    report_step: TimeDelta
    report_details_level: int


# Retrieve incidents from the PagerDuty service based on the provided arguments.
# :return: tuple of lists: collected, filtered_out.
def collect_incidents(
    args: Args,
    session: requests.Session,
) -> tuple[list[Incident], list[Incident]]:
    collected: list[Incident] = []
    filtered_out: list[Incident] = []
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
            if is_outage(args.title_checks, incident.title, args.priority_checks, incident.priority):
                collected.append(incident)
            else:
                filtered_out.append(incident)
    return collected, filtered_out


# Retrieve alerts from the PagerDuty service based on the provided arguments.
# :return: tuple of lists: collected, simplified, merged.
def collect_and_merge_alerts(
    args: Args,
    session: requests.Session,
    cache: Cache,
    incidents: list[Incident],
) -> tuple[list[Alert], list[Alert], list[Alert]]:
    collected: list[Alert] = []
    simplified: list[Alert] = []

    max_workers = min(8, os.cpu_count() or 1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(call_pagerduty_list_alerts_for_an_incident, cache, session, args.api_token, incident.id)
            for incident in incidents
        ]

    for index, future in enumerate(concurrent.futures.as_completed(futures)):
        f_collected = future.result()
        f_collected.sort(key=lambda alert: (alert.created, -alert.total_seconds()))

        f_merged = merge_overlapping_alerts(f_collected)
        if len(f_merged) == 1:
            f_merged[0] = Alert(
                ids=[incidents[index].id],
                created=f_merged[0].created,
                resolved=f_merged[0].resolved,
            )

        collected.extend(f_collected)
        simplified.extend(f_merged)

    simplified.sort(key=lambda alert: (alert.created, -alert.total_seconds()))
    merged = merge_overlapping_alerts(simplified)

    return collected, simplified, merged


# Print the final report about uptime.
def report_uptime(
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    alerts: list[Alert],
    report_details_level: int,
) -> None:
    duration = (end_date - start_date).total_seconds()
    downtime = sum(alert.total_seconds() for alert in alerts)
    uptime = (1 - (downtime / duration)) * 100
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
    return {"default": value} if value else {"required": True}


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
        type=parse_service_id,
        nargs="+",
        required=True,
        help=(
            "Services for which the script will perform calculations.\n"
            "Values can be service ID (e.g., ABCDEF4) or\n"
            "service URL (e.g., https://some.pagerduty.com/service-directory/ABCDEF4)."
        ),
    )
    arg_parser.add_argument(
        "--title-checks",
        metavar="PATTERN",
        dest="title_checks",
        type=functools.partial(re.compile, flags=0),
        nargs="*",
        required=False,
        help=(
            "Regular expressions for matching titles, e.g., '^Downtime', '^Outage'.\n"
            "The event title must match any of the provided regular expressions to be considered downtime.\n"
            "If none are specified, title matching is not used for downtime determination."
        ),
    )
    arg_parser.add_argument(
        "--priority-checks",
        metavar="PRIORITY",
        dest="priority_checks",
        type=str,
        nargs="*",
        required=False,
        help=(
            "Values for checking priority, e.g., 'P1', 'P2'.\n"
            "The event priority must match any of the specified values to be considered downtime.\n"
            "If none are specified, priority is not used for downtime determination."
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
    logging.info("title_checks=%s", args.title_checks)
    logging.info("priority_checks=%s", args.priority_checks)
    logging.info("incidents_since=%s", args.incidents_since)
    logging.info("incidents_until=%s", args.incidents_until)
    logging.info("report_step=%s", args.report_step)
    logging.info("report_details_level=%s", args.report_details_level)

    with requests.Session() as session, Cache() as cache:
        incidents_collected, incidents_filtered_out = collect_incidents(args, session)
        alerts_collected, alerts_simplified, alerts_merged = collect_and_merge_alerts(
            args, session, cache, incidents_collected
        )

    logging.info("len(incidents_collected)=%d", len(incidents_collected))
    logging.info("len(incidents_filtered_out)=%d", len(incidents_filtered_out))
    logging.debug("incidents_collected=%s", incidents_collected)
    logging.debug("incidents_filtered_out=%s", incidents_filtered_out)
    logging.info("len(alerts_collected)=%d", len(alerts_collected))
    logging.info("len(alerts_simplified)=%d", len(alerts_simplified))
    logging.info("len(alerts_merged)=%d", len(alerts_merged))
    logging.debug("alerts_merged=%s", alerts_merged)

    for interval_since, interval_until in intervals_gen(args.incidents_since, args.incidents_until, args.report_step):
        interval_alerts = filter_alerts(interval_since, interval_until, alerts_merged)
        report_uptime(interval_since, interval_until, interval_alerts, args.report_details_level)

    return 0


if __name__ == "__main__":
    sys.exit(main())
