# tests/unit/orchestration/test_dags.py
import os
import sys
from datetime import timedelta
from typing import TYPE_CHECKING, Any, List, Set
from unittest.mock import MagicMock

import pytest
from airflow.models import DagBag

if TYPE_CHECKING:
    from airflow.models import DAG, BaseOperator

# =========================================================================
# DOCKER COMPOSE PROFILE ENVIRONMENT DETECTOR
# =========================================================================
# 1. Grab the active compose configuration strings injected by Docker Desktop/Daemon
COMPOSE_PROFILE = os.getenv("COMPOSE_PROFILES", "").lower()
AIRFLOW_PROFILE = os.getenv("AIRFLOW_PROFILES", "").lower()

# 2. Determine if a live production big data cluster tier is active in the mesh.
# If we are running under a "full-stack" profile, we skip mocks to enforce strict validation.
IS_FULL_STACK_ACTIVE = any(
    p in COMPOSE_PROFILE or p in AIRFLOW_PROFILE
    for p in ["full-stack", "production", "integration"]
)

if not IS_FULL_STACK_ACTIVE:
    # We are inside a lean "unit-test", "offline", or baseline "dev" compose canvas.
    # Dynamically inject structural mock stubs to keep the inner test loop database-free.
    mock_spark_mod = MagicMock()
    from airflow.models.baseoperator import BaseOperator

    def mock_init(self: Any, *args: Any, **kwargs: Any) -> None:
        """Synthetic constructor to capture parameters without loading heavy JVM utilities."""
        base_kwargs = {k: v for k, v in kwargs.items() if k in ["task_id", "execution_timeout"]}
        BaseOperator.__init__(self, *args, **base_kwargs)

        if "retries" in kwargs:
            self.retries = kwargs["retries"]
        if "retry_exponential_backoff" in kwargs:
            self.retry_exponential_backoff = kwargs["retry_exponential_backoff"]

    SparkSubmitOperatorType = type("SparkSubmitOperator", (BaseOperator,), {"__init__": mock_init})
    mock_spark_mod.SparkSubmitOperator = SparkSubmitOperatorType

    # Inject directly into sys.modules to satisfy the python import lookup tree globally
    sys.modules["airflow.providers.apache"] = MagicMock()
    sys.modules["airflow.providers.apache.spark"] = MagicMock()
    sys.modules["airflow.providers.apache.spark.operators"] = MagicMock()
    sys.modules["airflow.providers.apache.spark.operators.spark_submit"] = mock_spark_mod


# =========================================================================
# ORCHESTRATION CONFIGURATION MATRIX LOOKUPS
# =========================================================================


@pytest.fixture(scope="session")
def dag_bag() -> DagBag:
    """Session-scoped fixture compiling local repository DAG topology profiles."""
    return DagBag(dag_folder="src/dags", include_examples=False, read_dags_from_db=False)


@pytest.mark.orchestration
def test_dag_bag_compilation_errors(dag_bag: DagBag) -> None:
    """U-DAG-02: Asserts that all graphs parse without syntax or import errors."""
    import_errors = dag_bag.import_errors
    error_msg = "\n".join([f"File: {f} -> Error: {e}" for f, e in import_errors.items()])
    assert len(import_errors) == 0, f"Airflow compilation drift tracked:\n{error_msg}"


def _get_active_dags(dag_folder: str = "src/dags") -> List[str]:
    """Helper vector to fetch compiled pipeline IDs for parameterisation."""
    bag = DagBag(dag_folder=dag_folder, include_examples=False, read_dags_from_db=False)
    return list(bag.dags.keys())


def _resolve_task_param(
    task: "BaseOperator", dag: "DAG", param_key: str, default: Any = None
) -> Any:
    """
    Unified Parameter Resolution Engine.

    Traverses the true Airflow configuration hierarchy:
    1. Direct task instance attributes
    2. Task local default_args dictionary overrides context
    3. Global DAG-level default_args dictionary boundaries
    """
    val = getattr(task, param_key, None)
    if val is not None:
        return val

    if hasattr(task, "default_args") and task.default_args:
        val = task.default_args.get(param_key, None)
        if val is not None:
            return val

    if dag.default_args and param_key in dag.default_args:
        return dag.default_args.get(param_key)

    return default


@pytest.mark.orchestration
@pytest.mark.parametrize("dag_id", _get_active_dags())
def test_dag_graph_mechanics_and_defenses_matrix(dag_bag: DagBag, dag_id: str) -> None:
    """
    U-DAG-01, U-DAG-03, U-DAG-04: Parameterised matrix tracking deadlocks,
    task identity collisions, and defensive production parameters.
    """
    dag: "DAG" = dag_bag.dags[dag_id]
    assert dag is not None, f"Failed to retrieve structural graph mapping for: {dag_id}"

    # 1. U-DAG-01: Pure In-Memory Topological Cycle / Deadlock Verification
    visited: Set[str] = set()
    rec_stack: Set[str] = set()

    def detect_cycle_dfs(task_id: str) -> bool:
        visited.add(task_id)
        rec_stack.add(task_id)

        task_node = dag.get_task(task_id)
        for downstream_task in task_node.downstream_list:
            if downstream_task.task_id not in visited:
                if detect_cycle_dfs(downstream_task.task_id):
                    return True
            elif downstream_task.task_id in rec_stack:
                return True

        rec_stack.remove(task_id)
        return False

    for task in dag.tasks:
        if task.task_id not in visited:
            assert detect_cycle_dfs(task.task_id) is False, (
                f"Orchestration Deadlock: A circular dependency cycle "
                f"was detected starting at task '{task.task_id}' inside {dag_id}!"
            )

    # 2. U-DAG-03: Unique Task Name Guard
    task_ids = [task.task_id for task in dag.tasks]
    assert len(task_ids) == len(set(task_ids)), f"Duplicate task_id inside: {dag_id}"

    # 3. U-DAG-04: Defensive Ingestion Parameters Verification
    for task in dag.tasks:
        task_name = task.task_id.lower()
        if any(
            keyword in task_name
            for keyword in ["scrape", "ingest", "extract", "stage", "match", "silver"]
        ):
            task_retries = _resolve_task_param(task, dag, "retries")
            is_backoff = _resolve_task_param(task, dag, "retry_exponential_backoff", default=False)
            task_timeout = _resolve_task_param(task, dag, "execution_timeout")

            assert task_retries is not None and task_retries >= 1, (
                f"Defensive Error: Task '{task.task_id}' in DAG '{dag_id}' "
                f"requires at least 1 retry allocation to bypass transient drops."
            )

            assert is_backoff is True, (
                f"Defensive Error: Task '{task.task_id}' in DAG '{dag_id}' "
                f"lacks retry_exponential_backoff activation parameters."
            )

            assert task_timeout is not None, (
                f"Defensive Error: Task '{task.task_id}' in DAG '{dag_id}' "
                f"lacks an execution_timeout property to prevent thread hanging."
            )

            assert task_timeout <= timedelta(minutes=1), (
                f"Defensive Error: Task '{task.task_id}' "
                "timeout exceeds the 1-minute safety threshold."
            )
