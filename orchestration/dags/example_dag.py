"""Smoke-test DAG: verifies scheduler, executor, and Postgres metadata DB wiring."""

from airflow.sdk import dag, task


@dag(schedule="@daily", catchup=False, tags=["example"])
def example_dag():
    @task
    def extract() -> list[int]:
        return [1, 2, 3]

    @task
    def summarize(values: list[int]) -> None:
        print(f"sum={sum(values)} count={len(values)}")

    summarize(extract())  # ty: ignore[invalid-argument-type]  (taskflow passes XComArg at parse time)


example_dag()
