# OpsSentinel

OpsSentinel is an event-driven, asynchronous, stateful logistics exception orchestration system built as decoupled microservices communicating via a local Kafka-like event bus.

## Services

1. Event Producer (`ops_sentinel/services/event_producer.py`)
2. Event Bus (`ops_sentinel/bus/event_bus.py`)
3. Event Processor Service (`ops_sentinel/services/event_processor_service.py`)
4. Agent Service (`ops_sentinel/services/agent_service.py`)
5. State Store (`ops_sentinel/state_store/state_store.py`)
6. Exception Engine (`ops_sentinel/services/exception_engine.py`)
7. Action Service (`ops_sentinel/services/action_service.py`)

## Event Flow

- `ingest.raw`
- `events.processed`
- `exceptions.detected`
- `actions.decided`
- `actions.executed`
- `exceptions.resolved`

Each service publishes and consumes asynchronously through topics only.

## Event Contract

All services enforce the strict event contract at `Schemas/event.schema.json` and in runtime validation at `ops_sentinel/contracts/event_contract.py`:

- required fields:
  - `event_id`, `event_type`, `source`, `timestamp`
  - `entity`, `related_entities`
  - `severity`, `text`, `signals`, `context`, `confidence`
  - `schema_version`
- enum constraints:
  - `entity.type`: `truck | shipment | order`
  - `severity`: `low | medium | high | critical`
- `schema_version` must be `1.0`

## Run

```bash
python3 main.py --max-events 40 --interval-s 0.15 --grace-s 1.0
```

The run prints a summary including topic sizes, consumer offsets, tracked entities, exceptions, and actions.
