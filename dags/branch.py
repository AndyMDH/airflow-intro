from airflow.sdk import dag, task


@dag(dag_id="branch")
def branch():
    @task
    def a():
        return 1

    @task.branch
    def b(val: int):
        if val == 1:
            return "equal_1"
        return "different_1"

    @task
    def equal_1(val: int):
        print("Equal to {val}")

    @task
    def different_1(val: int):
        print("Different than {val}")

    val = a()
    b(val) >> [equal_1(val), different_1(val)]


branch()
