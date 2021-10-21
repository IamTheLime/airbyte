#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import math
from abc import ABC, abstractmethod
from typing import Any, Iterable, Mapping, MutableMapping, Optional

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
        **kwargs
    ):
        super().__init__(**kwargs)
        self.access_token = access_token
        self.start_date = start_date
        self.environment = environment
        if self.environment == "sandbox":
            self.url_base = "https://api-sandbox.gocardless.com"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        decoded_response = response.json()
        if (
            len(decoded_response.get("resources", [])) > 0
        ):
            return {"starting_after": decoded_response["meta"]["cursors"]["after"]}

        return None

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
        yield from response_json.get("resources", [])


# Basic incremental stream
class IncrementalGocardlessStream(GocardlessStream, ABC):

    state_checkpoint_interval = math.inf

    def __init__(self, lookback_window_days: int = 0, **kwargs):
        super().__init__(**kwargs)
        self.lookback_window_days = lookback_window_days

    @property
    def cursor_field(self) -> str:
        """
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """

        return self.cursor_field

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {
            self.cursor_field: max(
                latest_record.get(self.cursor_field),
                current_stream_state.get(self.cursor_field, 0)
            )
        }

    def request_params(self, stream_state: Mapping[str, Any] = None, **kwargs):
        stream_state = stream_state or {}
        params = super().request_params(stream_state=stream_state, **kwargs)

        start_timestamp = self.get_start_timestamp(stream_state)
        if start_timestamp:
            params["created[gte]"] = start_timestamp
        return params

    def get_start_timestamp(self, stream_state) -> int:
        start_point = self.start_date
        if stream_state and self.cursor_field in stream_state:
            start_point = max(start_point, stream_state[self.cursor_field])

        if start_point and self.lookback_window_days:
            self.logger.info(f"Applying lookback window of {self.lookback_window_days} days to stream {self.name}")
            start_point = int(pendulum.from_timestamp(start_point).subtract(days=abs(self.lookback_window_days)).timestamp())

        return start_point


class Payments(IncrementalGocardlessStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    cursor = "created_at"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "customers"


# class ChargeBacks(IncrementalGocardlessStream):

