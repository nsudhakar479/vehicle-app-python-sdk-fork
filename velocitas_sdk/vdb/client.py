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

import asyncio
import logging
from typing import Dict, List, Optional
from urllib.parse import urlparse

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from velocitas_sdk import config
from velocitas_sdk.proto.broker_pb2 import (
    GetDatapointsRequest,
    GetMetadataRequest,
    SetDatapointsRequest,
    SubscribeRequest,
)
from velocitas_sdk.proto.broker_pb2_grpc import BrokerStub
from velocitas_sdk.proto.val2_pb2 import (
    ActuateRequest,
    BatchActuateRequest,
    GetValuesRequest,
    ListMetadataRequest,
)
from velocitas_sdk.proto.val2_pb2 import SubscribeRequest as ValV2SubscribeRequest
from velocitas_sdk.proto.val2_pb2_grpc import VALStub
from velocitas_sdk.proto.val2_types_pb2 import (
    BoolArray,
    DoubleArray,
    FloatArray,
    Int32Array,
    Int64Array,
    SignalID,
    StringArray,
    Uint32Array,
    Uint64Array,
    Value,
)
logger = logging.getLogger(__name__)


class VehicleDataBrokerClient:
    """VehicleDataBrokerClient provides the Graph API to access vehicle services
    and vehicle signals."""

    _instance = None

    def __new__(cls, port: Optional[int] = None):
        if cls._instance is None:
            cls._instance = super(VehicleDataBrokerClient, cls).__new__(cls)
            service_locator = config.middleware.service_locator
            _location = service_locator.get_service_location("vehicledatabroker")
            _hostname = urlparse(_location).hostname
            if port is None:
                _port = urlparse(_location).port
            else:
                _port = port

            _address = f"{_hostname}:{_port}"
            cls._channel = grpc.aio.insecure_channel(_address)  # type: ignore

            metadata = service_locator.get_metadata("vehicledatabroker")
            cls._metadata = metadata

            cls._stub = BrokerStub(cls._channel)
        return cls._instance

    async def close(self):
        """Closes runtime gRPC channel."""
        if self._channel:
            await self._channel.close()  # type: ignore

    def __enter__(self) -> "VehicleDataBrokerClient":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        asyncio.run_coroutine_threadsafe(self.close(), asyncio.get_event_loop())

    async def GetDatapoints(self, datapoints: List[str]):
        try:
            response = await self._stub.GetDatapoints(
                GetDatapointsRequest(datapoints=datapoints), metadata=self._metadata
            )
            return response
        except grpc.aio.AioRpcError:  # type: ignore
            logger.exception(
                "Error occured in VehicleDataBrokerClient.GetDatapoints",
            )
            raise

    async def SetDatapoints(self, datapoints):
        try:
            response = await self._stub.SetDatapoints(
                SetDatapointsRequest(datapoints=datapoints), metadata=self._metadata
            )
            return response
        except grpc.aio.AioRpcError:  # type: ignore
            logger.exception(
                "Error occured in VehicleDataBrokerClient.SetDatapoints",
            )
            raise

    def Subscribe(self, query: str):
        try:
            response = self._stub.Subscribe(
                SubscribeRequest(query=query),
                metadata=self._metadata,
            )
            return response
        except grpc.aio.AioRpcError:  # type: ignore
            logger.exception(
                "Error occured in VehicleDataBrokerClient.Subscribe",
            )
            raise

    async def GetMetadata(self, names: list):
        try:
            response = await self._stub.GetMetadata(
                GetMetadataRequest(names=names), metadata=self._metadata
            )
            return response
        except grpc.aio.AioRpcError:  # type: ignore
            logger.exception(
                "Error occured in VehicleDataBrokerClient.GetMetadata",
            )
            raise


class _ArrayView:
    """Lightweight stand-in for the legacy `FooArray` message's `.values`."""

    def __init__(self, values: list):
        self.values = values


_VAL2_ARRAY_CLASSES = {
    "bool_array": BoolArray,
    "string_array": StringArray,
    "int32_array": Int32Array,
    "int64_array": Int64Array,
    "uint32_array": Uint32Array,
    "uint64_array": Uint64Array,
    "float_array": FloatArray,
    "double_array": DoubleArray,
}

# Maps legacy sdv.databroker.v1.Datapoint oneof field names to the
# corresponding kuksa.val.v2.Value oneof field name.
_LEGACY_FIELD_TO_VAL2_TYPED_VALUE = {
    "string_value": "string",
    "bool_value": "bool",
    "int32_value": "int32",
    "int64_value": "int64",
    "uint32_value": "uint32",
    "uint64_value": "uint64",
    "float_value": "float",
    "double_value": "double",
    "bool_array": "bool_array",
    "string_array": "string_array",
    "int32_array": "int32_array",
    "int64_array": "int64_array",
    "uint32_array": "uint32_array",
    "uint64_array": "uint64_array",
    "float_array": "float_array",
    "double_array": "double_array",
}

_VAL2_SCALAR_DEFAULTS = {
    "string_value": "",
    "bool_value": False,
    "int32_value": 0,
    "int64_value": 0,
    "uint32_value": 0,
    "uint64_value": 0,
    "float_value": 0.0,
    "double_value": 0.0,
}

_VAL2_ARRAY_FIELDS = frozenset(
    name for name in _LEGACY_FIELD_TO_VAL2_TYPED_VALUE if name.endswith("_array")
)


def _broker_datapoint_to_val2_value(broker_datapoint) -> Value:
    """Converts a legacy sdv.databroker.v1.Datapoint into a kuksa.val.v2.Value."""
    which = broker_datapoint.WhichOneof("value")
    if which is None:
        raise ValueError("Datapoint has no value set")

    typed_value_name = _LEGACY_FIELD_TO_VAL2_TYPED_VALUE[which]
    if which in _VAL2_ARRAY_FIELDS:
        values = list(getattr(broker_datapoint, which).values)
        array_cls = _VAL2_ARRAY_CLASSES[which]
        return Value(**{typed_value_name: array_cls(values=values)})

    return Value(**{typed_value_name: getattr(broker_datapoint, which)})


class _Val2DatapointAdapter:
    """Exposes a kuksa.val.v2.Datapoint using the legacy Datapoint attribute
    names (e.g. `bool_value`, `int32_array.values`, ...) so existing model.py
    and reply.py code can consume VAL v2 responses unmodified."""

    def __init__(self, datapoint=None):
        self._datapoint = datapoint

    @property
    def timestamp(self) -> Timestamp:
        if self._datapoint is not None and self._datapoint.HasField("timestamp"):
            return self._datapoint.timestamp
        return Timestamp()

    def __getattr__(self, name: str):
        typed_value_name = _LEGACY_FIELD_TO_VAL2_TYPED_VALUE.get(name)
        if typed_value_name is None:
            raise AttributeError(name)

        is_array = name in _VAL2_ARRAY_FIELDS
        default = _ArrayView([]) if is_array else _VAL2_SCALAR_DEFAULTS[name]

        if self._datapoint is None or not self._datapoint.HasField("value"):
            return default

        value = self._datapoint.value
        if value.WhichOneof("typed_value") != typed_value_name:
            return default

        result = getattr(value, typed_value_name)
        return _ArrayView(list(result.values)) if is_array else result


class _Val2GetDatapointsResponse:
    def __init__(self, datapoints: Dict[str, "_Val2DatapointAdapter"]):
        self.datapoints = datapoints


class _Val2SetDatapointsResponse:
    def __init__(self, errors: Dict[str, str]):
        self.errors = errors


class _Val2SubscribeReply:
    def __init__(self, fields: Dict[str, "_Val2DatapointAdapter"]):
        self.fields = fields


class _Val2MetadataEntry:
    def __init__(self, name: str):
        self.name = name


class _Val2GetMetadataResponse:
    def __init__(self, entries: List["_Val2MetadataEntry"]):
        self.list = entries


def _parse_query_paths(query: str) -> List[str]:
    """Extracts the comma separated signal paths from a `SELECT ... [WHERE ...]`
    query string built by model.Node.get_query(). VAL v2 does not support the
    legacy JOIN/WHERE query syntax, so any WHERE clause is ignored and logged.
    """
    body = query
    if body.upper().startswith("SELECT "):
        body = body[len("SELECT ") :]

    where_index = body.upper().find(" WHERE ")
    if where_index != -1:
        logger.warning(
            "WHERE clauses are not supported by the kuksa.val.v2 backend, "
            "ignoring condition in query: %s",
            query,
        )
        body = body[:where_index]

    return [path.strip() for path in body.split(",") if path.strip()]


class VehicleDataBrokerClientValV2:
    """VehicleDataBrokerClientValV2 provides the GetDatapoints/SetDatapoints/
    Subscribe/GetMetadata facade used by model.py and subscriptions.py, backed
    by the kuksa.val.v2 gRPC API (VAL service) instead of the legacy
    sdv.databroker.v1 API."""

    _instance = None

    def __new__(cls, port: Optional[int] = None):
        if cls._instance is None:
            cls._instance = super(VehicleDataBrokerClientValV2, cls).__new__(cls)
            service_locator = config.middleware.service_locator
            _location = service_locator.get_service_location("vehicledatabroker")
            _hostname = urlparse(_location).hostname
            _port = port if port is not None else urlparse(_location).port

            _address = f"{_hostname}:{_port}"
            cls._channel = grpc.aio.insecure_channel(_address)  # type: ignore

            cls._metadata = service_locator.get_metadata("vehicledatabroker")
            cls._stub = VALStub(cls._channel)
        return cls._instance

    async def close(self):
        """Closes runtime gRPC channel."""
        if self._channel:
            await self._channel.close()  # type: ignore

    def __enter__(self) -> "VehicleDataBrokerClientValV2":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        asyncio.run_coroutine_threadsafe(self.close(), asyncio.get_event_loop())

    async def GetDatapoints(self, datapoints: List[str]):
        try:
            signal_ids = [SignalID(path=path) for path in datapoints]
            response = await self._stub.GetValues(
                GetValuesRequest(signal_ids=signal_ids), metadata=self._metadata
            )
            if len(response.data_points) != len(datapoints):
                raise RuntimeError(
                    "VAL v2 GetValues returned "
                    f"{len(response.data_points)} data points for "
                    f"{len(datapoints)} requested signals"
                )
            result = {
                path: _Val2DatapointAdapter(data_point)
                for path, data_point in zip(datapoints, response.data_points)
            }
            return _Val2GetDatapointsResponse(result)
        except grpc.aio.AioRpcError:  # type: ignore
            logger.exception(
                "Error occured in VehicleDataBrokerClientValV2.GetDatapoints",
            )
            raise

    async def SetDatapoints(self, datapoints):
        errors: Dict[str, str] = {}
        paths = list(datapoints.keys())
        actuate_requests = [
            ActuateRequest(
                signal_id=SignalID(path=path),
                value=_broker_datapoint_to_val2_value(broker_datapoint),
            )
            for path, broker_datapoint in datapoints.items()
        ]

        try:
            await self._stub.BatchActuate(
                BatchActuateRequest(actuate_requests=actuate_requests),
                metadata=self._metadata,
            )
            logger.debug("BatchActuate succeeded for %s", paths)
        except grpc.aio.AioRpcError as actuate_error:  # type: ignore
            logger.error(
                "BatchActuate failed for %s: %s %s",
                paths,
                actuate_error.code(),
                actuate_error.details(),
            )
            error_message = actuate_error.details() or str(actuate_error.code())
            errors = {path: error_message for path in paths}
        return _Val2SetDatapointsResponse(errors)

    def Subscribe(self, query: str):
        paths = _parse_query_paths(query)
        stream = self._stub.Subscribe(
            ValV2SubscribeRequest(signal_paths=paths),
            metadata=self._metadata,
        )
        return self._wrap_subscription(stream)

    async def _wrap_subscription(self, stream):
        latest_fields: Dict[str, "_Val2DatapointAdapter"] = {}
        async for response in stream:
            for path, data_point in response.entries.items():
                latest_fields[path] = _Val2DatapointAdapter(data_point)
            yield _Val2SubscribeReply(dict(latest_fields))

    async def GetMetadata(self, names: list):
        try:
            response = await self._stub.ListMetadata(
                ListMetadataRequest(root="Vehicle"), metadata=self._metadata
            )
            entries = [
                _Val2MetadataEntry(metadata.path)
                for metadata in response.metadata
                if not names or metadata.path in names
            ]
            return _Val2GetMetadataResponse(entries)
        except grpc.aio.AioRpcError:  # type: ignore
            logger.exception(
                "Error occured in VehicleDataBrokerClientValV2.GetMetadata",
            )
            raise


def get_vdb_client():
    """Returns the VehicleDataBroker client implementation selected via the
    `SDV_DATABROKER_API` environment variable (defaults to the legacy
    sdv.databroker.v1 API)."""
    if config.databroker_api == config.DATABROKER_API_KUKSA_VAL_V2:
        return VehicleDataBrokerClientValV2()
    return VehicleDataBrokerClient()
