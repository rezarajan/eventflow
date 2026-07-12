# Standalone Spool

Standalone mode supports:

```text
HTTP -> SQLite spool -> HTTP destination
```

This mode is for small single-instance deployments and incident handling. SQLite is local coordination and replay state, not a distributed broker. Do not scale replicas over a shared SQLite volume.
