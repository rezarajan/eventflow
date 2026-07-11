# Twelve-Factor Runtime

Commands read configuration from explicit flags and `EVENTFLOW_*` environment
variables.

Logs go to stderr. Commands that stream events write only event data to stdout.
Startup validates required resource and adapter configuration. Long-running
commands should handle SIGINT and SIGTERM through context cancellation.
