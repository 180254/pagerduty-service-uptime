What is this:
    pagerduty-service-uptime

Installation of required packages:
    sudo apt-get install python3 python3-pip

Installation of required dependencies:
    pip3 install --user --upgrade -r requirements.txt

Usage:
    ./pagerduty-service-uptime.py --help

Usage example:
    PAGERDUTY_API_TOKEN="some-key-11"
    ./pagerduty-service-uptime.py \
        --log-level WARN \
        --api-token "$PAGERDUTY_API_TOKEN" \
        --service-id "https://some.pagerduty.com/services/ABCDEF4" \
        --incidents-since "2019-01-01T00:00:00Z" \
        --incidents-until "2020-01-01T00:00:00Z" \
        --report-step "1 month"

PagerDuty api key:
    https://support.pagerduty.com/docs/generating-api-keys#section-generating-a-personal-rest-api-key
