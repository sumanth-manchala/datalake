# AGENTS instructions

## Testing Standards

- Target exactly 100% coverage of what the PR changes — no more, no less. Every changed or added behaviour must have a test; every test must fail without the PR's change. Do not add tests for pre-existing logic that was already present before the PR, and do not test standard-library or third-party functions. The exception is deliberate behaviour or integration tests, which may cross those boundaries by design.
- Use pytest patterns, not `unittest.TestCase`.
- Use `spec`/`autospec` when mocking.
- Prefer `@mock.patch` decorators over `with mock.patch(...)` context managers for patching. Use `conf_vars` (from `tests_common.test_utils.config`) for Airflow config overrides — as a decorator when the value is fixed, as a context manager when it varies via `@pytest.mark.parametrize`.
- Use `time_machine` for time-dependent tests. Do not use `datetime.now()`
- Use `@pytest.mark.parametrize` for multiple similar inputs — consolidate tests that only differ in input/expected values into a single parametrized test.