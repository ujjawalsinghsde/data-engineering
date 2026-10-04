# Unit Testing PySpark Projects With Pytest

## 1. Why Unit Testing Matters

Unit testing means testing small units of code independently.

In PySpark projects, unit tests help verify:

- readers load expected data
- transformations produce expected output
- filters are correct
- joins do not break
- aggregations return expected counts
- configs are read correctly

If code is modular, each function can be tested separately.

## 2. Why Pytest

Python has built-in `unittest`, but `pytest` is widely preferred because:

- simpler syntax
- fixtures are clean
- parameterization is easy
- markers help organize tests
- strong ecosystem

Install:

```bash
pipenv install pytest --dev
```

Run:

```bash
python -m pytest
```

Verbose:

```bash
python -m pytest -v
```

## 3. Test File Naming

Pytest discovers files that:

- start with `test_`
- or end with `_test.py`

Examples:

```text
test_retail_project.py
retail_project_test.py
```

Test functions should also start with `test_`.

## 4. Pytest Config

`pytest.ini`

```ini
[pytest]
markers =
    slow: mark a test as slow
    transformation: mark a test as a transformation
    latest: tests currently under active development
```

Why define markers?

Without marker registration, pytest can warn about unknown markers.

## 5. Spark Fixture

Setup code should not be repeated in every test.

Use fixtures.

`tests/conftest.py`

```python
import pytest
from lib.Utils import get_spark_session


@pytest.fixture
def spark():
    spark_session = get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop()
```

The `yield` pattern supports:

```text
setup -> test runs -> teardown
```

Here:

- setup creates SparkSession
- test uses SparkSession
- teardown stops SparkSession

## 6. Expected Results Fixture

For aggregation tests, keep expected output in a test data file.

Example:

```text
data/test_result/state_aggregate.csv
```

`conftest.py`:

```python
@pytest.fixture
def expected_results(spark):
    results_schema = "state string, count int"

    return spark.read \
        .format("csv") \
        .schema(results_schema) \
        .load("data/test_result/state_aggregate.csv")
```

## 7. Reader Tests

```python
import pytest
from lib.DataReader import read_customers, read_orders


@pytest.mark.skip("work in progress")
def test_read_customers_df(spark):
    customers_count = read_customers(spark, "LOCAL").count()
    assert customers_count == 12435


@pytest.mark.skip("work in progress")
def test_read_orders_df(spark):
    orders_count = read_orders(spark, "LOCAL").count()
    assert orders_count == 68883
```

Use `skip` when a test is not ready.

Do not leave important production tests skipped forever.

## 8. Transformation Test

```python
from lib.DataReader import read_orders
from lib.DataManipulation import filter_closed_orders


@pytest.mark.transformation
def test_filter_closed_orders(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_count = filter_closed_orders(orders_df).count()
    assert filtered_count == 7556
```

This verifies that the `filter_closed_orders` transformation returns expected rows.

## 9. Parameterized Test

Instead of writing three separate tests, parameterize:

```python
import pytest
from lib.DataReader import read_orders
from lib.DataManipulation import filter_orders_generic


@pytest.mark.parametrize(
    "status, expected_count",
    [
        ("CLOSED", 7556),
        ("PENDING_PAYMENT", 15030),
        ("COMPLETE", 22899),
    ],
)
@pytest.mark.latest
def test_check_order_status_count(spark, status, expected_count):
    orders_df = read_orders(spark, "LOCAL")
    filtered_count = filter_orders_generic(orders_df, status).count()
    assert filtered_count == expected_count
```

Benefits:

- less duplicate code
- easier to add more cases
- clearer test intent

## 10. Config Test

```python
from lib.ConfigReader import get_app_config


@pytest.mark.slow
def test_read_app_config():
    config = get_app_config("LOCAL")
    assert config["orders.file.path"] == "data/orders.csv"
```

This verifies that config parsing works.

## 11. Aggregation Test

```python
from lib.DataReader import read_customers, read_orders
from lib.DataManipulation import (
    filter_closed_orders,
    join_orders_customers,
    count_orders_state,
)


def test_count_orders_state(spark, expected_results):
    orders_df = read_orders(spark, "LOCAL")
    customers_df = read_customers(spark, "LOCAL")

    closed_orders_df = filter_closed_orders(orders_df)
    joined_df = join_orders_customers(closed_orders_df, customers_df)
    actual_results = count_orders_state(joined_df)

    actual = set(tuple(row) for row in actual_results.collect())
    expected = set(tuple(row) for row in expected_results.collect())

    assert actual == expected
```

Why use `set`?

Spark does not guarantee row order unless you explicitly sort.

## 12. Running Marker-Based Tests

Run only latest tests:

```bash
python -m pytest -m latest
```

Run only transformation tests:

```bash
python -m pytest -m transformation
```

Show fixtures:

```bash
python -m pytest --fixtures
```

Show markers:

```bash
python -m pytest --markers
```

## 13. What To Unit Test In PySpark

Good unit test targets:

- parsing and cleaning functions
- filters
- `withColumn` logic
- null replacement
- column renaming
- business rules
- joins on small sample data
- aggregations on deterministic input
- config loading

Avoid unit testing:

- Spark internals
- full production-scale data
- cluster performance
- external systems in pure unit tests

Use integration tests for end-to-end behavior.

## 14. Common Mistakes

1. Creating SparkSession in every test manually.
2. Forgetting to stop SparkSession.
3. Comparing unordered DataFrames directly.
4. Testing only row counts and not content.
5. Using production data paths in unit tests.
6. Leaving all tests skipped.
7. Writing one giant test for the entire pipeline.

## 15. Production Testing Strategy

Recommended layers:

```text
Unit tests:
  transformation logic on tiny data

Integration tests:
  read sample files, run small pipeline

Data quality tests:
  validate production input/output rules

Performance tests:
  run representative volume in test/stage
```

## 16. Interview Questions

### Beginner

1. What is unit testing?
2. Why use pytest?
3. What is a fixture?
4. What is `conftest.py`?
5. How do you run pytest?

### Intermediate

1. How do you test PySpark transformations?
2. Why use parameterized tests?
3. How do markers help?
4. Why should SparkSession be a fixture?
5. How do you compare Spark DataFrame output?

### Senior

1. How would you design tests for a large PySpark pipeline?
2. How do you separate unit tests from integration tests?
3. How do you handle nondeterministic row ordering?
4. How do you test data quality rules?
5. How do tests fit into CI/CD for Spark jobs?

## 17. Quick Revision

- Use pytest for simple and powerful tests.
- Put SparkSession setup in `conftest.py`.
- Use `yield` fixtures for teardown.
- Use parameterization to reduce duplicate tests.
- Use markers to run selected tests.
- Compare unordered Spark outputs carefully.
