
------------------------------
## 🔒 Tier 2: Pre-Push Integration Test Registry (I-DAG-xx)

* Scope: Validates dynamic string compilation, Jinja macro execution, and runtime ecosystem variable lookups.
* Environment: Executed during your pre-push gate loop (pytest -m integration) with your stateful Postgres metadata database and MinIO container mesh fully online.

| ID | Focus Area | Type | Expected Behaviour / Key Assertion |
|---|---|---|---|
| I-DAG-01 | Jinja Template Rendering | Negative | Initializes the core system metadata DB (airflow db init), seeds mock platform variables, and force-renders all task parameter strings. |
