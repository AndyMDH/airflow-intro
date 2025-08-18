from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
    
@dag(dag_id="user_processing")
def user_processing():
    
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    
    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        try:
            import requests
            response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
            print(f"API Response Status: {response.status_code}")
            if response.status_code == 200:
                condition = True
                fake_user = response.json()
            else:
                condition = False
                fake_user = None
            return PokeReturnValue(is_done=condition, xcom_value=fake_user)
        except Exception as e:
            print(f"Error checking API: {e}")
            return PokeReturnValue(is_done=False, xcom_value=None)
    
    @task
    def extract_user(fake_user):
        if fake_user is None:
            raise ValueError("No user data received from API")
        return {
            "id": fake_user["id"],
            "firstname": fake_user["personalInfo"]["firstName"],
            "lastname": fake_user["personalInfo"]["lastName"],
            "email": fake_user["personalInfo"]["email"],
        }
        
    @task
    def process_user(user_info):
        import csv
        from datetime import datetime
        
        # Use the user_info from the previous task instead of hardcoding
        if user_info is None:
            # Fallback to default data if no user_info provided
            user_info = {
                "id": "123",
                "firstname": "John",
                "lastname": "Doe",
                "email": "john.doe@example.com",
            }
        
        user_info["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open("/tmp/user_info.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "firstname", "lastname", "email", "created_at"])
            writer.writeheader()
            writer.writerow(user_info)
        return user_info
            
    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id="postgres")
        hook.copy_expert(
            sql="COPY users FROM STDIN WITH CSV HEADER",
            filename="/tmp/user_info.csv"
        )
    
    process_user(extract_user(create_table >> is_api_available())) >> store_user()
            
user_processing()