# KUKSA VAL v2 Support

The SDK can talk to a [KUKSA Databroker](https://github.com/eclipse-kuksa/kuksa-databroker)
using either of two wire protocols:

| Backend                              | `SDV_DATABROKER_API` value | Notes                                   |
| ------------------------------------- | --------------------------- | ---------------------------------------- |
| `sdv.databroker.v1` (legacy, default) | `sdv-databroker-v1`          | Existing `BrokerStub`-based client.       |
| `kuksa.val.v2`                       | `kuksa-val-v2`               | New `VALStub`-based client (this change). |

No application code changes are required to switch backends: `Vehicle.*.get()`,
`.set()`, `.subscribe()` and `set_many()` all work unmodified, because the VAL v2
client (`velocitas_sdk.vdb.client.VehicleDataBrokerClientValV2`) exposes the same
`GetDatapoints` / `SetDatapoints` / `Subscribe` / `GetMetadata` facade as the legacy
client and adapts kuksa.val.v2 messages into the legacy attribute shape
(`bool_value`, `int32_array.values`, ...) expected by `model.py` and `vdb/reply.py`.

## Enabling VAL v2

Set the following environment variables before starting a Vehicle App:

```bash
export SDV_DATABROKER_API=kuksa-val-v2
export SDV_VEHICLEDATABROKER_ADDRESS=grpc://localhost:55555
```

`velocitas_sdk.config.databroker_api` reflects the selected backend, and
`velocitas_sdk.vdb.client.get_vdb_client()` is the factory used internally by
`model.Node.get_client()` to pick the right implementation.

## Implementation notes

- Proto sources: [`velocitas_sdk/proto/kuksa_val_v2/val2.proto`](../velocitas_sdk/proto/kuksa_val_v2/val2.proto)
  and [`val2_types.proto`](../velocitas_sdk/proto/kuksa_val_v2/val2_types.proto),
  mirroring the upstream `kuksa.val.v2` wire format. Generated stubs
  (`val2_pb2.py`, `val2_pb2_grpc.py`, `val2_types_pb2.py`) are produced the same
  way as the legacy stubs, see [`generate-grpc-stubs.sh`](../generate-grpc-stubs.sh).
- `GetDatapoints`/`GetValues` → `VAL.GetValues`.
- `SetDatapoints` → `VAL.Actuate`, falling back to `VAL.PublishValue` for signals
  that are not actuators (sensors/attributes).
- `Subscribe` → `VAL.Subscribe`. The legacy `SELECT ... WHERE ...` query syntax
  used by `Node.get_query()` is parsed for its signal paths; `WHERE` filter
  expressions are not supported by kuksa.val.v2 and are ignored with a warning.
- `GetMetadata` → `VAL.ListMetadata`.

## Limitations

- `WHERE`/`JOIN` subscription filters (sdv.databroker.v1 query language) are not
  supported by kuksa.val.v2 and are dropped when subscribing.
- Provider-side APIs (`OpenProviderStream`, `ProvideActuation`, `PublishValues`
  streaming) are not wired up; this change only covers the Vehicle App
  (consumer) side described in the architecture diagram.
