#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

import pendulum
import pytest
from source_gocardless.streams import Payments

now_dt = pendulum.now()

SECONDS_IN_DAY = 24 * 60 * 60


@pytest.mark.parametrize(
    "lookback_window_days, current_state, expected, message",
    [
        (None, now_dt.to_iso8601_string(), now_dt.to_iso8601_string(), "if lookback_window_days is not set should not affect cursor value"),
        (0, now_dt.to_iso8601_string(), now_dt.to_iso8601_string(), "if lookback_window_days is not set should not affect cursor value"),
        (10, now_dt.to_iso8601_string(), now_dt.subtract(seconds=SECONDS_IN_DAY * 10).to_iso8601_string(), "Should calculate cursor value as expected"),
        # ignore sign
        (-10, now_dt.to_iso8601_string(), now_dt.subtract(seconds=SECONDS_IN_DAY * 10).to_iso8601_string(), "Should not care for the sign, use the module"),
    ],
)
def test_lookback_window(lookback_window_days, current_state, expected, message):
    payment_stream = Payments(
        access_token=213,
        environment="sandbox",
        start_date="2017-01-25T00:00:00Z",
        lookback_window_days=lookback_window_days,
        gocardless_version="2015-07-06"
    )
    payment_stream.cursor_field = "created_at"
    assert payment_stream.get_start_timestamp({"created_at": current_state}) == expected, message
