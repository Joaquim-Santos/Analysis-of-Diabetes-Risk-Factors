import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from minio import Minio
from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import NearMiss

DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 1, 1),
}

dag = DAG('etl_class_balance',
          default_args=DEFAULT_ARGS,
          schedule_interval="@once"
          )

data_lake_server = Variable.get("data_lake_server")
data_lake_login = Variable.get("data_lake_login")
data_lake_password = Variable.get("data_lake_password")

client = Minio(
    data_lake_server,
    access_key=data_lake_login,
    secret_key=data_lake_password,
    secure=False
)


def extract():
    # extrai os dados a partir do Data Lake.
    client.fget_object(
        "processing",
        "df_diabetes_dummy.parquet",
        "/tmp/df_diabetes_dummy.parquet"
    )
    df = pd.read_parquet("/tmp/df_diabetes_dummy.parquet")

    # persiste o dataset na área de Staging.
    df.to_csv("/tmp/df_diabetes_dummy.csv", index=False)


def transform():
    # ler os dados a partir da área de Staging.
    df_diabetes_dummy = pd.read_csv("/tmp/df_diabetes_dummy.csv")
    X = df_diabetes_dummy.drop(["diabetes"], axis=1)
    y = df_diabetes_dummy['diabetes']

    # Realizando balanceamento de classes.
    nm = NearMiss(sampling_strategy=0.40)
    x_nm, y_nm = nm.fit_resample(X, y)

    smote = SMOTE(sampling_strategy=0.60, random_state=33)
    x_smote, y_smote = smote.fit_resample(x_nm, y_nm)

    # persiste os dados transformados na área de staging.
    x_smote['diabetes'] = y_smote
    x_smote.to_csv("/tmp/balanced_dataset.csv", index=False)


def load():
    # carrega os dados a partir da área de staging.
    df = pd.read_csv("/tmp/balanced_dataset.csv")

    # converte os dados para o formato parquet.
    df.to_parquet("/tmp/balanced_dataset.parquet", index=False)

    # carrega os dados para o Data Lake.
    client.fput_object(
        "processing",
        "balanced_dataset.parquet",
        "/tmp/balanced_dataset.parquet"
    )


extract_task = PythonOperator(
    task_id='extract_file_from_data_lake',
    python_callable=extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_file_to_data_lake',
    python_callable=load,
    dag=dag
)

clean_task = BashOperator(
    task_id="clean_files_on_staging",
    bash_command="rm -f /tmp/*.csv;rm -f /tmp/*.parquet;",
    dag=dag
)

extract_task >> transform_task >> load_task >> clean_task
