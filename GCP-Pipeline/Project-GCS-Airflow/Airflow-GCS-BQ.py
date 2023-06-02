import pandas as pd
import requests
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


MYSQL_CONNECTION = "mysql_df"  # Id connection MySQL in Airflow 
Infocomm_Rankings_URL = "https://golink.icu/apidata"
# API 

# path  
mysql_output_path = "/home/airflow/gcs/data/data_merged.csv"
infocomm_top_output_path = "/home/airflow/gcs/data/infocomm.api.csv"
final_output_path = "/home/airflow/gcs/data/convert.output.csv"

def get_mysql(transaction_path):
    # transaction_path from task

    # Use MySqlHook connection MySQL in Airflow
    mysqlserver = MySqlHook(MYSQL_CONNECTION)
    
    # Query from database use the hook that creates a pandas DataFrame
    audible_data = mysqlserver.get_pandas_df(sql="SELECT * FROM audible_data")
    audible_transaction = mysqlserver.get_pandas_df(sql="SELECT * FROM audible_transaction")

    # Merge data 2 DataFrame
    df = audible_transaction.merge(audible_data, how="left", left_on="book_id", right_on="Book_ID")

    # Save file CSV to transaction_path (merged)
    df.to_csv(transaction_path, index=False)
    print(f"Output to {transaction_path}")


def get_data_files(conversion_data_path):
    r = requests.get(Infocomm_Rankings_URL) # API
    result_infocomm_top = r.json()
    df = pd.DataFrame(result_infocomm_top)

    # Add column name date save CSV
    df = df.reset_index().rename(columns={"index": "date"})
    df.to_csv(conversion_data_path, index=False)
    print(f"Output to {conversion_data_path}")


def merge_data(transaction_path, conversion_data_path, output_path):
    transaction = pd.read_csv(transaction_path)
    infocomm_top = pd.read_csv(conversion_data_path)

    transaction['date'] = transaction['timestamp']
    transaction['date'] = pd.to_datetime(transaction['date']).dt.date
    infocomm_top['date'] = pd.to_datetime(infocomm_top['date']).dt.date

    # merge 2 DataFrame
    final_df = transaction.merge(infocomm_top, how="left", left_on="date", right_on="date")

    # Convert the price by removing the $ sign and converting it to a float
    final_df["Price"] = final_df.apply(lambda x: x["Price"].replace("$",""), axis=1)
    final_df["Price"] = final_df["Price"].astype(float)

    final_df["THBPrice"] = final_df["Price"] * final_df["conversion_rate"]
    final_df = final_df.drop(["date", "book_id"], axis=1)
    
    # save CSV
    final_df.to_csv(output_path, index=False)
    print(f"Output to {output_path}")
    
with DAG(
    "incomm_top",
    start_date=days_ago(1),
    schedule_interval="@once",
    tags=["workshop"]
) as dag:

    t1 = PythonOperator(
        task_id="get_mysql",
        python_callable=get_mysql,
        op_kwargs={
            "transaction_path": mysql_output_path,
        },
    )

    t2 = PythonOperator(
        task_id="get_data_files",
        python_callable=get_data_files,
        op_kwargs={
            "conversion_data_path": infocomm_top_output_path,
        },
    )

    t3 = PythonOperator(
        task_id="merge_data",
        python_callable=merge_data,
        op_kwargs={
            "transaction_path": mysql_output_path,
            "conversion_data_path": infocomm_top_output_path,
            "output_path" : final_output_path,
        },
    )

    t4 = BashOperator(
        task_id="load_to_bq",
        bash_command="bq load --source_format=CSV --autodetect workshop.infocomm_data gs://asia-east2-workshop-75098a63-bucket/data/output.csv"
    )

    [t1, t2] >> t3 >> t4

