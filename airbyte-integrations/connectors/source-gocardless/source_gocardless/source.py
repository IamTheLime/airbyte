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
        auth = TokenAuthenticator(token=config["access_token"])
        start_date = config["start_date"]
        args = {
            "authenticator": auth,
            "access_token": config["access_token"],
            "environment":config["gocardless_environment"],
            "start_date": start_date,
            "gocardless_version": config["gocardless_version"],
        }
        incremental_args = {**args, "lookback_window_days": config.get("lookback_window_days")}
        return [
            Payments(**incremental_args)
        ]
