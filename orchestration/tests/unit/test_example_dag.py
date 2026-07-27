"""Unit test: the static example DAG imports cleanly and has the expected shape."""

import example_dag as example_dag_module


def test_example_dag_definition():
    dag = example_dag_module.example_dag()
    assert dag.dag_id == "example_dag"
    assert not dag.catchup
    assert {t.task_id for t in dag.tasks} == {"extract", "summarize"}
