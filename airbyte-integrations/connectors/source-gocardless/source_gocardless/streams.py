#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import logging
import math
from abc import ABC, abstractmethod
from typing import Any, Callable, Iterable, Mapping, MutableMapping, Optional

import pendulum
import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.http import HttpStream

class GocardlessStream(HttpStream, ABC):

    # TODO: Fill in the url base. Required.
    url_base = "https://api.gocardless.com/"
    primary_key = "id"

    def __init__(
        self,
        start_date: int,
        access_token: str,
        environment: str,
        gocardless_version: str,
        req_data_access_keyword: str, # DEFINE THIS IN THE SUBCLASS, i.e. Payment
        **kwargs
    ):
        super().__init__(**kwargs)
        self.access_token = access_token
        self.start_date = start_date
        self.environment = environment
        self.gocardless_version = gocardless_version
        self.req_data_access_keyword = req_data_access_keyword

        if self.environment == "sandbox":
            self.url_base = "https://api-sandbox.gocardless.com/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        decoded_response = response.json()
        # These cursors are referent to cursor pagination
        if (decoded_response["meta"]["cursors"]["after"] != None):
            return {"after": decoded_response["meta"]["cursors"]["after"]}

        return None

    def request_headers(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return {
            "GoCardless-Version": self.gocardless_version,
        }

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:

        # Gocardless API sets a limit of 500 to any list of results
        stream_handler = {
            "limit": 500,
        }

        if next_page_token:
            stream_handler.update(next_page_token)

        return stream_handler

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        # We need to pass on the gocardless_req_access_keyword argument as each of the
        # list methods will return the results in the body of the request under a
        # camel case keyword of the stream class, i.e. CustomerBankAccounts -> customer_bank_accounts
        yield from response_json.get(self.req_data_access_keyword, [])

class GocardlessEventStream(HttpStream, ABC):

    # TODO: Fill in the url base. Required.
    url_base = "https://api.gocardless.com/"
    primary_key = "id"

    def __init__(
        self,
        start_date: int,
        access_token: str,
        environment: str,
        gocardless_version: str,
        req_primary_data_accessor: Callable,
        req_primary_sort_func: Callable,
        req_secondary_data_accessor: Callable,
        req_secondary_sort_func : Callable,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.access_token = access_token
        self.start_date = start_date
        self.environment = environment
        self.gocardless_version = gocardless_version

        self.req_primary_data_accessor = req_primary_data_accessor
        self.req_primary_sort_func = req_primary_sort_func
        self.req_secondary_data_accessor = req_secondary_data_accessor
        self.req_secondary_sort_func = req_secondary_sort_func

        if self.environment == "sandbox":
            self.url_base = "https://api-sandbox.gocardless.com/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        decoded_response = response.json()
        # These cursors are referent to cursor pagination
        if (decoded_response["meta"]["cursors"]["after"] != None):
            return {"after": decoded_response["meta"]["cursors"]["after"]}

        return None

    def request_headers(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return {
            "GoCardless-Version": self.gocardless_version,
        }

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:

        # Gocardless API sets a limit of 500 to any list of results
        stream_handler = {
            "limit": 500,
        }

        if next_page_token:
            stream_handler.update(next_page_token)

        return stream_handler

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        # We need to pass on the gocardless_req_access_keyword argument as each of the
        # list methods will return the results in the body of the request under a
        # camel case keyword of the stream class, i.e. CustomerBankAccounts -> customer_bank_accounts

        response_json["events"] = sorted(self.req_primary_data_accessor(response_json), key=self.req_primary_sort_func)
        linked_values = response_json["linked"]
        response_json["payments"] = sorted(linked_values["payments"], self.req_secondary_sort_func)

        event_list = [{**event[0], "payment": event[1]} for event in list(zip(self.req, events["payments"]))]

        yield from response_json.get(self.req_data_access_keyword, [])


# Basic incremental stream
class IncrementalGocardlessEventStream(GocardlessEventStream, ABC):

    state_checkpoint_interval = math.inf

    def __init__(self, lookback_window_days: int = 0, **kwargs):
        super().__init__(**kwargs)
        self.lookback_window_days = lookback_window_days

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """

        latest_record_ts = pendulum.parse(latest_record.get(self.cursor_field))
        current_state_ts = pendulum.parse(current_stream_state.get(self.cursor_field, "1970-01-01T00:00:00.000Z"))

        updated_state = max(latest_record_ts, current_state_ts).to_iso8601_string()

        return {
            self.cursor_field: updated_state
        }

    def request_params(self, stream_state: Mapping[str, Any] = None, **kwargs):
        stream_state = stream_state or {}
        params = super().request_params(stream_state=stream_state, **kwargs)

        start_timestamp = self.get_start_timestamp(stream_state)
        if start_timestamp:
            params["created_at[gte]"] = start_timestamp
        return params

    def get_start_timestamp(self, stream_state) -> int:
        start_point = self.start_date
        if stream_state and self.cursor_field in stream_state:
            start_point = max(start_point, stream_state[self.cursor_field])

        if start_point and self.lookback_window_days:
            self.logger.info(f"Applying lookback window of {self.lookback_window_days} days to stream {self.name}")
            start_point = pendulum.parse(start_point).subtract(days=abs(self.lookback_window_days)).to_iso8601_string()

        return start_point


class Payments(GocardlessStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    def __init__(
        self,
        **kwargs
    ):
        super().__init__(**kwargs, req_data_access_keyword="payments")

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        should return "payments". Required.
        """
        return "payments"


class PaymentEvents(IncrementalGocardlessEventStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    cursor_field = "created_at"

    def __init__(
        self,
        **kwargs
    ):
        super().__init__(
            **kwargs,
            req_primary_data_accessor=lambda event: event["events"],
            req_primary_sort_func=lambda resource: resource["links"]["payment"],
            req_secondary_data_accessor=lambda event: event["payments"],
            req_secondary_sort_func=lambda resource: resource["id"],
        )

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        Check events API for information on event listing
        """
        return "events?resource_type=payments&include=payment"