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

The kuksa-val-v2 backend always writes via VAL.Actuate/BatchActuate, which
the Databroker rejects with INVALID_ARGUMENT for signals that aren't
actuators (e.g. Vehicle.Speed is a sensor). The set()/set_many() demos
below therefore use actuator paths (Vehicle.Cabin.Seat.Row1.DriverSide.*),
and require an actuation provider claiming those signals to be running,
e.g. the seatservice example (task "run-seatservice" / seatservice.py in
this repo) which registers itself as the provider for the seat signals.

Usage:
    export SDV_MIDDLEWARE_TYPE=native
    export SDV_DATABROKER_API=kuksa-val-v2       # or sdv-databroker-v1
    export SDV_VEHICLEDATABROKER_ADDRESS=grpc://localhost:55555
    # start a provider for the seat actuators, e.g.: ./.vscode/scripts/run-seatservice.sh
    python3 main.py
"""

import asyncio
import logging

from velocitas_sdk import config
from velocitas_sdk.model import (
    DataPointFloat,
    DataPointInt32,
    DataPointUint8,
    Model,
)
from velocitas_sdk.vdb.reply import DataPointReply

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DriverSide(Model):
    def __init__(self, parent):
        super().__init__(parent)
        # Actuators: require a provider (e.g. seatservice) claiming these signals.
        self.Position = DataPointInt32("Position", self)
        self.Recline = DataPointInt32("Recline", self)


class Row1(Model):
    def __init__(self, parent):
        super().__init__(parent)
        self.DriverSide = DriverSide(self)


class Seat(Model):
    def __init__(self, parent):
        super().__init__(parent)
        self.Row1 = Row1(self)


class Driver(Model):
    def __init__(self, parent):
        super().__init__(parent)
        self.FanSpeed = DataPointUint8("FanSpeed", self)


class Row1HVAC(Model):
    def __init__(self, parent):
        super().__init__(parent)
        self.name = "Row1"
        self.Driver = Driver(self)


class Station(Model):
    def __init__(self, parent):
        super().__init__(parent)
        self.Row1 = Row1HVAC(self)


class HVAC(Model):
    def __init__(self, parent):
        super().__init__(parent)
        self.Station = Station(self)


class Cabin(Model):
    def __init__(self, parent):
        super().__init__(parent)
        self.Seat = Seat(self)
        self.HVAC = HVAC(self)


class Vehicle(Model):
    def __init__(self):
        super().__init__()
        self.Speed = DataPointFloat("Speed", self)  # sensor, read-only here
        self.Cabin = Cabin(self)


async def on_position_update(reply: DataPointReply, vehicle: Vehicle) -> None:
    logger.info(
        "subscribe() update -> Seat.Row1.DriverSide.Position = %s",
        reply.get(vehicle.Cabin.Seat.Row1.DriverSide.Position).value,
    )


async def main() -> None:
    vehicle = Vehicle()
    logger.info("Using databroker API: %s", config.databroker_api)

    logger.info("--- get() ---")
    logger.info("Vehicle.Speed = %s", (await vehicle.Speed.get()).value)

    logger.info("--- set() (actuator, requires a running provider e.g. seatservice) ---")
    await vehicle.Cabin.Seat.Row1.DriverSide.Position.set(42)
    logger.info(
        "Seat.Row1.DriverSide.Position after set = %s",
        (await vehicle.Cabin.Seat.Row1.DriverSide.Position.get()).value,
    )

    logger.info("--- set_many() (actuators, requires providers for seat + hvac) ---")
    await (
        vehicle.set_many()
        .add(vehicle.Cabin.Seat.Row1.DriverSide.Recline, 10)
        .add(vehicle.Cabin.HVAC.Station.Row1.Driver.FanSpeed, 3)
        .apply()
    )
    logger.info(
        "Recline = %s, FanSpeed = %s",
        (await vehicle.Cabin.Seat.Row1.DriverSide.Recline.get()).value,
        (await vehicle.Cabin.HVAC.Station.Row1.Driver.FanSpeed.get()).value,
    )

    logger.info("--- subscribe() for 5 seconds ---")

    async def on_position_update_callback(reply: DataPointReply) -> None:
        await on_position_update(reply, vehicle)

    subscription = await vehicle.Cabin.Seat.Row1.DriverSide.Position.subscribe(
        on_position_update_callback
    )
    await vehicle.Cabin.Seat.Row1.DriverSide.Position.set(88)
    await asyncio.sleep(5)
    await subscription.unsubscribe()

    logger.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())
