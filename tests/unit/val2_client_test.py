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

"""Tests for the kuksa.val.v2 backed VehicleDataBrokerClientValV2 and its
legacy-compatible adapters."""

import os
from unittest import mock

import grpc
import pytest

from velocitas_sdk import config
from velocitas_sdk.config import Config
from velocitas_sdk.proto.types_pb2 import Datapoint as BrokerDatapoint
from velocitas_sdk.proto.types_pb2 import Int32Array as BrokerInt32Array
from velocitas_sdk.proto.val2_pb2 import GetValuesResponse, ListMetadataResponse
from velocitas_sdk.proto.val2_types_pb2 import Datapoint, Int32Array, Metadata, Value
from velocitas_sdk.vdb.client import (
    VehicleDataBrokerClientValV2,
    _broker_datapoint_to_val2_value,
    _parse_query_paths,
    _Val2DatapointAdapter,
    get_vdb_client,
)


@pytest.fixture(autouse=True)
def reset():
    config._config = Config("native")
    config.middleware = config._config.middleware
    config.databroker_api = config.DATABROKER_API_SDV_V1
    VehicleDataBrokerClientValV2._instance = None
    os.environ.pop("SDV_DATABROKER_API", None)


def test_datapoint_adapter_scalar():
    adapter = _Val2DatapointAdapter(Datapoint(value=Value(int32=42)))
    assert adapter.int32_value == 42
    # Accessing an unset oneof member must yield the legacy default.
    assert adapter.bool_value is False


def test_datapoint_adapter_array():
    adapter = _Val2DatapointAdapter(
        Datapoint(value=Value(int32_array=Int32Array(values=[1, 2, 3])))
    )
    assert list(adapter.int32_array.values) == [1, 2, 3]
    assert list(adapter.string_array.values) == []


def test_datapoint_adapter_no_value():
    adapter = _Val2DatapointAdapter(None)
    assert adapter.int32_value == 0
    assert list(adapter.bool_array.values) == []


def test_broker_datapoint_to_val2_value_scalar():
    value = _broker_datapoint_to_val2_value(BrokerDatapoint(int32_value=7))
    assert value.WhichOneof("typed_value") == "int32"
    assert value.int32 == 7


def test_broker_datapoint_to_val2_value_array():
    value = _broker_datapoint_to_val2_value(
        BrokerDatapoint(int32_array=BrokerInt32Array(values=[4, 5]))
    )
    assert value.WhichOneof("typed_value") == "int32_array"
    assert list(value.int32_array.values) == [4, 5]


def test_parse_query_paths_strips_where_clause():
    paths = _parse_query_paths(
        "SELECT Vehicle.Speed, Vehicle.ADAS.CruiseControl.SpeedSet"
        " WHERE Vehicle.Speed > 0"
    )
    assert paths == ["Vehicle.Speed", "Vehicle.ADAS.CruiseControl.SpeedSet"]


@pytest.mark.asyncio
async def test_get_vdb_client_selects_backend_from_config():
    config.databroker_api = config.DATABROKER_API_KUKSA_VAL_V2
    client = get_vdb_client()
    assert isinstance(client, VehicleDataBrokerClientValV2)
    await client.close()


@pytest.mark.asyncio
async def test_get_datapoints():
    client = VehicleDataBrokerClientValV2()
    response = GetValuesResponse(data_points=[Datapoint(value=Value(int32=100))])
    with mock.patch.object(
        client._stub, "GetValues", new_callable=mock.AsyncMock, return_value=response
    ):
        result = await client.GetDatapoints(["Vehicle.Speed"])
        assert result.datapoints["Vehicle.Speed"].int32_value == 100
    await client.close()


@pytest.mark.asyncio
async def test_get_datapoints_rejects_response_with_missing_entries():
    client = VehicleDataBrokerClientValV2()
    response = GetValuesResponse(data_points=[])
    with mock.patch.object(
        client._stub, "GetValues", new_callable=mock.AsyncMock, return_value=response
    ):
        with pytest.raises(RuntimeError, match="returned 0 data points for 1"):
            await client.GetDatapoints(
                ["Vehicle.Powertrain.Transmission.PerformanceMode"]
            )
    await client.close()


@pytest.mark.asyncio
async def test_get_metadata():
    client = VehicleDataBrokerClientValV2()
    response = ListMetadataResponse(metadata=[Metadata(path="Vehicle.Speed")])
    with mock.patch.object(
        client._stub,
        "ListMetadata",
        new_callable=mock.AsyncMock,
        return_value=response,
    ):
        result = await client.GetMetadata([])
        assert result.list[0].name == "Vehicle.Speed"
    await client.close()


def _rpc_error(code):
    return grpc.aio.AioRpcError(
        code, initial_metadata=(), trailing_metadata=(), details=str(code)
    )


@pytest.mark.asyncio
async def test_set_datapoints_actuate_success():
    client = VehicleDataBrokerClientValV2()
    with (
        mock.patch.object(
            client._stub, "BatchActuate", new_callable=mock.AsyncMock
        ) as batch_actuate,
        mock.patch.object(
            client._stub, "PublishValue", new_callable=mock.AsyncMock
        ) as publish_value,
    ):
        result = await client.SetDatapoints(
            {
                "Vehicle.Body.Windshield.Front.Wiping.Mode": BrokerDatapoint(
                    string_value="ON"
                )
            }
        )
        assert not result.errors
        batch_actuate.assert_awaited_once()
        publish_value.assert_not_awaited()
    await client.close()


@pytest.mark.asyncio
async def test_set_datapoints_reports_invalid_argument_without_publish():
    client = VehicleDataBrokerClientValV2()
    with (
        mock.patch.object(
            client._stub,
            "BatchActuate",
            new_callable=mock.AsyncMock,
            side_effect=_rpc_error(grpc.StatusCode.INVALID_ARGUMENT),
        ),
        mock.patch.object(
            client._stub, "PublishValue", new_callable=mock.AsyncMock
        ) as publish_value,
    ):
        result = await client.SetDatapoints(
            {"Vehicle.Speed": BrokerDatapoint(float_value=1.0)}
        )
        assert result.errors["Vehicle.Speed"]
        publish_value.assert_not_awaited()
    await client.close()


@pytest.mark.asyncio
async def test_set_datapoints_reports_unavailable_without_publish():
    client = VehicleDataBrokerClientValV2()
    with (
        mock.patch.object(
            client._stub,
            "BatchActuate",
            new_callable=mock.AsyncMock,
            side_effect=_rpc_error(grpc.StatusCode.UNAVAILABLE),
        ),
        mock.patch.object(
            client._stub, "PublishValue", new_callable=mock.AsyncMock
        ) as publish_value,
    ):
        result = await client.SetDatapoints(
            {
                "Vehicle.Powertrain.Transmission.PerformanceMode": BrokerDatapoint(
                    string_value="SPORT"
                )
            }
        )
        assert result.errors["Vehicle.Powertrain.Transmission.PerformanceMode"]
        publish_value.assert_not_awaited()
    await client.close()


@pytest.mark.asyncio
async def test_set_datapoints_reports_permission_denied_without_fallback():
    client = VehicleDataBrokerClientValV2()
    with (
        mock.patch.object(
            client._stub,
            "BatchActuate",
            new_callable=mock.AsyncMock,
            side_effect=_rpc_error(grpc.StatusCode.PERMISSION_DENIED),
        ),
        mock.patch.object(
            client._stub, "PublishValue", new_callable=mock.AsyncMock
        ) as publish_value,
    ):
        result = await client.SetDatapoints(
            {
                "Vehicle.Powertrain.Transmission.PerformanceMode": BrokerDatapoint(
                    string_value="SPORT"
                )
            }
        )
        assert result.errors["Vehicle.Powertrain.Transmission.PerformanceMode"]
        publish_value.assert_not_awaited()
    await client.close()
