#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import gocardless_pro
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from source_gocardless.streams import Payments
import pendulum


# Source
class SourceGocardless(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            gocardless_pro.Client(
                access_token=config["access_token"],
                environment=config["gocardless_environment"],
            )

            return True, None

        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        # TODO remove the authenticator if not required.
        auth = TokenAuthenticator(token=config["access_token"])
        start_date = pendulum.parse(config["start_date"]).int_timestamp
        args = {
            "authenticator": auth,
            "access_token": config["access_token"],
            "environment":config["gocardless_environment"],
            "start_date": start_date
        }
        incremental_args = {**args, "lookback_window_days": config.get("lookback_window_days")}
        return [
            Payments(**incremental_args)
        ]
