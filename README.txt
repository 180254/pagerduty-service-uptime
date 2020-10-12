What is this:
    pagerduty-service-uptime
    Calculates system uptime based on incidents in PagerDuty.
    Allows to indicate which incidents are considered as downtime and affect the calculation of uptime.

Installation of required packages (Debian-based Linux):
    sudo apt-get install python3 python3-pip

Installation of required dependencies:
    pip3 install --user --upgrade -r requirements.txt

Usage:
    ./pagerduty_service_uptime.py --help

Usage example:
    PAGERDUTY_API_TOKEN="some-key-11"
    ./pagerduty_service_uptime.py \
        --log-level INFO \
        --api-token "$PAGERDUTY_API_TOKEN" \
        --service-ids "https://some.pagerduty.com/services/ABCDEF4" \
        --title-checks "^Downtime"
        --incidents-since "2019-01-01T00:00:00Z" \
        --incidents-until "2020-01-01T00:00:00Z" \
        --report-step "1 month" \
        --report-details-level 1

PagerDuty api key:
    https://support.pagerduty.com/docs/generating-api-keys#section-generating-a-personal-rest-api-key

Anything else you need to know:
    The script caches some PagerDuty API responses to a folder called ".cache" in the working directory.
