# Test Mode Notes

Current repository state intentionally runs in a simplified test mode.

## Why this exists

The team is running controlled test launches of the scraper, so some production-oriented components are intentionally disabled for now.

## Intentional deviations

- Scheduler startup is disabled in `src/main.py`.
- Batch polling is temporarily executed from the worker loop instead of APScheduler.
- Periodic jobs are not active in runtime:
  - `schedule_updates`
  - `recover_tasks`
  - `retry_stale_batches`
  - weekly discover job

## Expected impact

- No automatic periodic maintenance tasks in runtime.
- Only task polling and direct task processing are active.
- AI batch status checks still happen, but via worker loop polling.

## Re-enable plan

When test mode is finished:

1. Re-enable `create_scheduler(...)` and `scheduler.start()` in `src/main.py`.
2. Move batch polling responsibility back to scheduler jobs.
3. Restore and verify all periodic jobs for production timing.
4. Update architecture docs to remove temporary-mode notes.
