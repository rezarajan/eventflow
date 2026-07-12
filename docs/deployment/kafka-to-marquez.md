# Kafka To Marquez

Kafka-to-Marquez mode consumes already-published OpenLineage events from Redpanda or Kafka and forwards accepted events to a Marquez-compatible HTTP endpoint.

Kafka remains the durable source of truth. Eventflow commits the source offset only after successful Marquez delivery. If policy validation fails, the event is quarantined and the offset may be committed after the terminal rejection is recorded.

This mode avoids duplicating every Kafka event into SQLite. Kafka offsets and broker retention provide durability. Use Marquez idempotency support or idempotent sink behavior because redelivery after crashes is possible.
