from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests

MYSQL_CONNECTION = "mysql_default"    # Id connection MySQL in Airflow  
CONVERSION_RATE_URL = "https://golink.icu/apidata"

# path  
mysql_output_path = "/home/airflow/data/merged_data.csv"
api_output_path = "/home/airflow/data/api_data.csv"
final_output_path = "/home/airflow/data/transfers_data.csv"


def get_data_from_mysql(transaction_path):
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


def get_conversion_rate(conversion_api_path):
    r = requests.get(CONVERSION_RATE_URL) # API
    result_conversion_rate = r.json()
    df = pd.DataFrame(result_conversion_rate)

    # Add column name date save CSV
    df = df.reset_index().rename(columns={"index": "date"})
    df.to_csv(conversion_api_path, index=False)
    print(f"Output to {conversion_api_path}")


def merge_data(transaction_path, conversion_api_path, output_path):
    transaction = pd.read_csv(transaction_path)
    conversion_rate = pd.read_csv(conversion_api_path)

    transaction['date'] = transaction['timestamp']
    transaction['date'] = pd.to_datetime(transaction['date']).dt.date
    conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date

    # merge 2 DataFrame
    final_df = transaction.merge(conversion_rate, how="left", left_on="date", right_on="date")
    
    # Convert the price by removing the $ sign and converting it to a float
    final_df["Price"] = final_df.apply(lambda x: x["Price"].replace("$",""), axis=1)
    final_df["Price"] = final_df["Price"].astype(float)

    final_df["THBPrice"] = final_df["Price"] * final_df["conversion_rate"]
    final_df = final_df.drop(["date", "book_id"], axis=1)

    # save CSV
    final_df.to_csv(output_path, index=False)
    print(f"Output to {output_path}")
    print("== End of Workshop 4 ʕ•́ᴥ•̀ʔっ♡ ==")


with DAG(
    "data_final",
    start_date=days_ago(1),
    schedule_interval="@once",
    tags=["workshop"]
) as dag:

    
    
    t1 = PythonOperator(
        task_id="data_mysql",
        python_callable=get_data_from_mysql,
        op_kwargs={
            "transaction_path": mysql_output_path,
        },
    )

    t2 = PythonOperator(
        task_id="api_data",
        python_callable=get_conversion_rate,
        op_kwargs={
            "conversion_api_path": api_output_path,
        },
    )

    t3 = PythonOperator(
        task_id="merge_data",
        python_callable=merge_data,
        op_kwargs={
            "transaction_path": mysql_output_path,
            "conversion_api_path": api_output_path,
            "output_path" : final_output_path,
        },
    )

    [t1, t2] >> t3
