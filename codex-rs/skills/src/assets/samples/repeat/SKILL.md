---
name: repeat
description: Schedule reminders, recurring prompts, and polling tasks in the current Codex session. Use when the user wants something to happen later, on a cadence, or repeatedly until a condition is met, including reminders like "remind me at 3pm", deploy or build polling, PR babysitting, or listing, pausing, resuming, and canceling scheduled tasks.
metadata:
  short-description: Schedule reminders, loops, and polling tasks
---

# Repeat

Use this skill when the user wants delayed, recurring, or poll-until-done work inside the
current Codex thread.

## Feature Gate

- If the scheduling tools are unavailable in this session, explain that scheduling is disabled and
  tell the user to enable:

```toml
[features]
scheduler = true
```

- Then stop. Do not pretend the schedule was created.

## Tool Map

- `schedule_create`: create a durable scheduled task. Always set `durable: true`.
- `schedule_list`: list schedules for the current thread.
- `schedule_delete`: cancel a schedule by id.
- `schedule_pause`: pause a schedule by id.
- `schedule_resume`: resume a paused schedule by id.
- `schedule_report`: only for the scheduled poll execution itself when the injected scheduled-run
  instructions explicitly ask for it. Do not call `schedule_report` while creating, listing, or
  managing schedules.

## Choosing the Trigger

- Use `trigger: "time"` for one-shot reminders or recurring schedules that should run on the clock
  regardless of the result of prior runs.
- Use `trigger: "poll"` when the job should keep checking until a condition is satisfied or it
  times out.

### `time`

- Use `schedule.in` for delays like `in 45 minutes`, `in 2h`, or `in 1h 30m`.
- Use `schedule.at` for wall-clock times like `at 3pm tomorrow`. Convert them to an RFC3339
  timestamp in the session's local timezone.
- Use `schedule.every` for recurring schedules like `every 10m` or `every 2h`.

### `poll`

- Provide `poll.every` and `poll.until`.
- Add `poll.timeout` when the user gives an overall deadline such as `for 6 hours` or `until end
  of day`.
- Add `poll.max_runs` only when the user gives an explicit attempt limit.

If the user says both `every ...` and `until ...`, prefer `trigger: "poll"` when each run is
checking progress toward that condition.

## Parsing and Normalization

- Normalize duration words to compact duration strings that the tools accept:
  - `45 minutes` -> `45m`
  - `2 hours` -> `2h`
  - `1 hour 30 minutes` -> `1h 30m`
- If the user explicitly invokes `$repeat` as `[interval] <prompt>` or `<prompt> every <interval>`,
  create a recurring `time` schedule with that interval.
- If `$repeat` is invoked without any schedule phrase, default to `every 10m`.
- Preserve the runnable prompt after removing the scheduling phrase. Slash commands and skill
  mentions inside the prompt should pass through unchanged.
- For reminder requests, keep the prompt framed as a reminder instead of turning it into a direct
  action. Example: `remind me at 3pm to push the release branch` should schedule a prompt like
  `Remind me to push the release branch.`
- For check or babysit requests, schedule the actual task prompt. Example:
  `check whether the integration tests passed in 45 minutes` should schedule a prompt like
  `Check whether the integration tests passed and tell me what happened.`
- If the requested wall-clock time is ambiguous, ask a follow-up question instead of guessing.

## Defaults

- Default `concurrency` to `queue`.
- Default `misfire_policy` to `run_once`.

Only change those when the user explicitly asks for skipped missed runs or overlapping parallel
scheduled executions.

## Managing Existing Schedules

- For `what scheduled tasks do I have?`, call `schedule_list`.
- For `cancel`, `pause`, or `resume` requests:
  - If the user gives an id, use it directly.
  - If they describe the schedule instead, call `schedule_list`, pick the obvious match, and ask a
    follow-up question before mutating anything when there is ambiguity.
- When listing schedules, summarize each item with its id, status, next run time, and prompt.

## Confirmation Style

- After creating a schedule, confirm:
  - what will run
  - whether it is one-shot, recurring, or polling
  - the resolved time or cadence
  - the schedule id
  - that it is durable for this Codex environment
- Prefer exact dates and times in confirmations when the user used relative wording.

## Examples

- `$repeat 10m /review-pr 1234` -> recurring `time` schedule with `schedule.every: "10m"`.
- `remind me at 3pm to push the release branch` -> one-shot `time` schedule with `schedule.at`.
- `in 45 minutes, check whether the integration tests passed` -> one-shot `time` schedule with
  `schedule.in: "45m"`.
- `check the deploy every 5 minutes until it is healthy` -> `poll` schedule with
  `poll.every: "5m"` and `poll.until: "the deploy is healthy"`.
- `what scheduled tasks do I have?` -> `schedule_list`.
