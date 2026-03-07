# Run prompts on a schedule

Use `$repeat` and the scheduler tools to run prompts later, on a cadence, or until a condition is satisfied.

Scheduled tasks in Codex are thread-scoped and durable. They are stored under `CODEX_HOME/schedules/<thread-id>.json`, survive Codex restarts, and resume the next time the owning thread is active again.

## Enable scheduled tasks

Scheduling is guarded by the `scheduler` feature flag:

```toml
[features]
scheduler = true
```

See [Configuration](config.md#scheduler) for the runtime and storage details.

## Schedule a recurring prompt with `$repeat`

The bundled `$repeat` system skill is the quickest way to schedule recurring work.

```text
$repeat 5m check if the deployment finished and tell me what happened
```

Codex parses the interval, creates a durable schedule, and confirms the cadence and schedule id.

### Interval syntax

Intervals are optional. You can lead with them, trail them, or leave them out entirely.

| Form | Example | Parsed interval |
| :--- | :--- | :--- |
| Leading token | `$repeat 30m check the build` | every 30 minutes |
| Trailing `every` clause | `$repeat check the build every 2 hours` | every 2 hours |
| No interval | `$repeat check the build` | defaults to every 10 minutes |

Durations can be compact like `10m`, `2h`, or `1h 30m`, or natural language like `every 5 minutes`. The minimum supported interval is 10 seconds.

### Repeat another skill-driven workflow

The scheduled prompt can itself invoke another skill or established workflow.

```text
$repeat 20m $babysit-pr check PR 1234
```

Each time the schedule fires, Codex runs that prompt in a scheduled turn.

## Set a one-time reminder

You can ask naturally for one-shot reminders and delayed checks. Codex can select `$repeat` for you.

```text
remind me at 3pm to push the release branch
```

```text
in 45 minutes, check whether the integration tests passed
```

Codex resolves the time in your local timezone, creates a durable one-shot schedule, and deletes it after it runs.

## Poll until something is done

Poll schedules are for repeated checks that should stop once a condition is satisfied.

```text
check the deploy every 5 minutes until it is healthy
```

```text
$repeat watch the build every 30s until it succeeds or fails
```

Under the hood, these create `trigger: "poll"` schedules with `every`, `until`, and optional `timeout` or `max_runs` limits.

## Manage scheduled tasks

Ask naturally to inspect or change schedules:

```text
what scheduled tasks do I have?
```

```text
pause the deploy check schedule
```

```text
resume schedule 2f6cb4d2-7d4d-4e8c-9df0-bf2c7ab2b778
```

```text
cancel the release reminder
```

Each schedule has a UUID you can use with the underlying tools.

### Under the hood

| Tool | Purpose |
| :--- | :--- |
| `schedule_create` | Create a durable scheduled Codex task. Supports `time` triggers for `at`, `in`, and `every`, and `poll` triggers for `every ... until ...`. |
| `schedule_list` | List durable scheduled tasks for the current thread. |
| `schedule_delete` | Delete a scheduled task by id. |
| `schedule_pause` | Pause a scheduled task by id. |
| `schedule_resume` | Resume a paused scheduled task by id. |
| `schedule_report` | Internal poll-run reporting tool used by scheduled poll executions to record `continue`, `completed`, or `failed`. |

## How scheduled tasks run

The scheduler wakes roughly once per second and checks for due tasks.

Scheduled runs capture the turn settings from the moment they were created, including the working directory, approval policy, sandbox policy, reasoning-summary setting, service tier, and personality.

By default, schedules use `concurrency: "queue"`, which means a due task waits until the current thread is idle. You can also use:

- `skip` to drop a due occurrence if the thread is busy
- `parallel` to launch a child scheduled run in parallel

## Persistence and misfires

Schedules are durable, but missed intervals are not replayed one by one.

The default `misfire_policy` is `run_once`, which means Codex will run at most one catch-up occurrence after downtime or a long delay, then advance the schedule. Use `skip` if you want missed occurrences to be discarded instead.

For poll schedules, Codex can also stop automatically when a `timeout` or `max_runs` limit is reached.

## Limitations

- Schedules belong to the current thread. `schedule_list` only shows the current thread's schedules.
- Scheduling is unavailable unless the `scheduler` feature flag is enabled.
- Scheduled jobs cannot be created from Plan mode turns.
- The minimum supported interval is 10 seconds.
