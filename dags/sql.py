from airflow.sdk import dag, task
@dag(
    def sql_dag():
        @tasl.sql(
            conn_id="postgres"
            
        )
)