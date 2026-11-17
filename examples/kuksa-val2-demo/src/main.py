# Copyright (c) 2022-2025 Contributors to the Eclipse Foundation
#
# This program and the accompanying materials are made available under the
# terms of the Apache License, Version 2.0 which is available at
# https://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# SPDX-License-Identifier: Apache-2.0

"""Standalone smoke-test for the kuksa.val.v2 backend.

Exercises get(), set(), subscribe() and set_many() against a running
KUKSA Databroker, using whichever backend is selected via
SDV_DATABROKER_API (sdv-databroker-v1 or kuksa-val-v2). No MQTT broker or
generated vehicle model package is required.

Usage:
    export SDV_MIDDLEWARE_TYPE=native
    export SDV_DATABROKER_API=kuksa-val-v2       # or sdv-databroker-v1
    export SDV_VEHICLEDATABROKER_ADDRESS=grpc://localhost:55555
    python3 main.py
"""

import asyncio
import logging

from velocitas_sdk import config
from velocitas_sdk.model import (
    DataPointBoolean,
    DataPointFloat,
    DataPointInt32,
    DataPointString,
    DataPointStringArray,
    Model,
)
from velocitas_sdk.vdb.reply import DataPointReply

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Cabin(Model):
    def __init__(self, parent):
        super().__init__(parent)
        self.DriverSeatOccupied = DataPointBoolean("DriverSeatOccupied", self)


class Vehicle(Model):
    def __init__(self):
        super().__init__()
        self.Speed = DataPointFloat("Speed", self)
        self.VehicleIdentification = DataPointString("VehicleIdentification", self)
        self.TraveledDistance = DataPointInt32("TraveledDistance", self)
        self.Tags = DataPointStringArray("Tags", self)
        self.Cabin = Cabin(self)


async def on_speed_update(reply: DataPointReply, vehicle: Vehicle) -> None:
    logger.info("subscribe() update -> Vehicle.Speed = %s", reply.get(vehicle.Speed).value)


async def main() -> None:
    vehicle = Vehicle()
    logger.info("Using databroker API: %s", config.databroker_api)

    logger.info("--- get() ---")
    logger.info("Vehicle.Speed = %s", (await vehicle.Speed.get()).value)

    logger.info("--- set() ---")
    await vehicle.Speed.set(42.0)
    logger.info("Vehicle.Speed after set = %s", (await vehicle.Speed.get()).value)

    logger.info("--- set_many() ---")
    await (
        vehicle.set_many()
        .add(vehicle.VehicleIdentification, "demo-vin-001")
        .add(vehicle.TraveledDistance, 1234)
        .add(vehicle.Tags, ["demo", "val-v2"])
        .apply()
    )
    logger.info(
        "VehicleIdentification = %s, TraveledDistance = %s, Tags = %s",
        (await vehicle.VehicleIdentification.get()).value,
        (await vehicle.TraveledDistance.get()).value,
        (await vehicle.Tags.get()).value,
    )

    logger.info("--- subscribe() for 5 seconds ---")
    subscription = await vehicle.Speed.subscribe(
        lambda reply: on_speed_update(reply, vehicle)
    )
    await vehicle.Speed.set(88.0)
    await asyncio.sleep(5)
    await subscription.unsubscribe()

    logger.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())
