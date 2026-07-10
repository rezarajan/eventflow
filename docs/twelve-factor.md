# Twelve-Factor Runtime

Commands read configuration from explicit flags and `EVENTFLOW_*` environment
variables. Existing `DATASCAPE_*` aliases may remain temporarily for migration
and should be treated as deprecated.

Logs go to stderr. Commands that stream events write only event data to stdout.
Startup validates required registry and adapter configuration. Long-running
commands should handle SIGINT and SIGTERM through context cancellation.

