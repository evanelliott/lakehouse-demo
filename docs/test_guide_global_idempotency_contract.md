Since we’ve ditched the over-engineered "Meta-Framework" in favour of Explicit, Readable Code, the "Global Idempotency Contract" is no longer a shared utility, but a mandatory coding pattern.
Every transformation test follows a standard template that ensures your Airflow DAGs remain safe to retry, while keeping the mock data visible and the production signatures clean.
------------------------------
## 1. The Strategy: Pattern-Based Idempotency
Instead of a generic runner, we enforce a "Double-Act" pattern within each individual transformation test. This proves that the logic is convergent and side-effect-free using the actual production function signatures.

| Requirement | Implementation |
|---|---|
| Engine | Spark Connect (Remote Session) for parity or DuckDB for speed. |
| Mocking | Explicit, in-test createDataFrame or from_df calls (No hidden factories). |
| The Contract | The "Second Pass" must result in zero row changes or hash differences. |

------------------------------
## 2. The Test Template (Spark)
File: tests/unit/transforms/test_injury_history.py
This approach allows the function to keep its natural signature (including any number of lookups or broadcast variables).
```python
def test_injury_history_idempotency(spark):
    """
    U-TR-04: Verifies the SCD2 injury logic is idempotent.
    """
    # 1. ARRANGE: Explicitly defined mock data for transparency
    initial_state = spark.createDataFrame([...], schema=SCD2_SCHEMA)
    new_batch = spark.createDataFrame([...], schema=BRONZE_SCHEMA)
    lookup_df = spark.createDataFrame([...], schema=REF_SCHEMA)

    # 2. ACT: First Pass (The 'Write' run)
    # Use the actual production signature - no **kwargs faff.
    first_run = update_injury_history(initial_state, new_batch, lookup_df)
    
    # 3. ACT: Second Pass (The 'Idempotent' run)
    second_run = update_injury_history(first_run, new_batch, lookup_df)
    
    # 4. ASSERT: Byte-for-byte identity
    # We use collect() for small unit test data or exceptAll() for larger sets
    assert first_run.collect() == second_run.collect(), "State changed on second run!"
```
------------------------------
## 3. The Test Template (DuckDB)
File: tests/unit/transforms/test_team_metrics.py
```python
def test_metrics_idempotency(duck_con):
    """
    U-TR-08: Verifies Gold-layer metrics are deterministic.
    """
    # 1. ARRANGE
    initial_metrics = duck_con.from_df(pd.DataFrame([...]))
    silver_matches = duck_con.from_df(pd.DataFrame([...]))

    # 2. ACT
    pass_one = calculate_team_metrics(initial_metrics, silver_matches)
    pass_two = calculate_team_metrics(pass_one, silver_matches)
    
    # 3. ASSERT: SQL-based comparison
    diff = duck_con.execute("SELECT * FROM pass_one EXCEPT SELECT * FROM pass_two").fetchone()
    assert diff is None, "Idempotency failed: Metrics shifted on re-run."
```
------------------------------
## 4. Why this is the "No-Faff" Winner

   1. Zero Learning Curve: New developers don't need to learn a custom "Contract Framework." They just write standard pytest code.
   2. No "Hidden" Logic: When a test fails, you see exactly what data caused the failure right in the test file.
   3. Refactoring Friendly: If you change a function's arguments, you only update the test call. You don't have to rewire a global mapping dictionary.
   4. Auditability: It satisfies your Shift-Left requirement by proving every job in the repo is "Airflow-Safe" before it ever leaves the DevContainer.

## Summary of the Gate
The "Contract" is now a peer-review standard: "Does your unit test include the second-pass idempotency check?"
Ready to move to the Pre-push Integration suite and design how we test the MinIO / Iceberg REST / Docker plumbing?
Follow-up: Should we define the Negative Path integration tests first (e.g., how the system reacts when the Iceberg Catalog is unreachable)?

