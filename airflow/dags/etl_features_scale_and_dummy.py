import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from minio import Minio
from airflow.sensors.external_task import ExternalTaskSensor


DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 1, 1),
}

dag = DAG('etl_features_scale_and_dummy',
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


def map_imc_ranges(x):
    if x <= 18.49:
        return 'under_weight'
    elif x >= 18.50 and x <=24.99:
        return 'normal_weight'
    elif x >= 25 and x <=29.99:
        return 'overweight'
    elif x >= 30 and x <=34.99:
        return 'obesity_I'
    elif x >= 35 and x <=39.99:
        return 'obesity_II'
    elif x >= 40:
        return 'obesity_III'
    else:
        return 'NaN'


def map_age_ranges(x):
    switcher = {
        1: "below_30",
        2: "below_30",
        3: "30_39",
        4: "30_39",
        5: "40_49",
        6: "40_49",
        7: "50_59",
        8: "50_59",
        9: "60_69",
        10: "60_69",
        11: "above_70",
        12: "above_70",
        13: "above_70"
    }
    return switcher.get(x, 0)


def map_physical_health_days(x):
    if x == 0:
        return 'zero'
    elif x >= 1 and x <=7:
        return '1_7'
    elif x >= 8 and x <=13:
        return '8_13'
    elif x >= 14 and x <=21:
        return '14_21'
    elif x >= 22 and x <=30:
        return '22_30'
    else:
        return 'NaN'


def map_income_scale(x):
    switcher = {
        1: '1',
        2: '1',
        3: '2',
        4: '2',
        5: '3',
        6: '3',
        7: '4',
        8: '4'
    }
    return switcher.get(x, 0)


def map_health_scale(x):
    switcher = {
        1: '1',
        2: '1',
        3: '2',
        4: '3',
        5: '3'
    }
    return switcher.get(x, 0)


def extract():
    # extrai os dados a partir do Data Lake.
    client.fget_object(
        "processing",
        "df_diabetes_indcators.parquet",
        "/tmp/df_diabetes_indcators.parquet",
    )
    df = pd.read_parquet("/tmp/df_diabetes_indcators.parquet")

    # persiste o dataset na área de Staging.
    df.to_csv("/tmp/df_diabetes_indcators.csv", index=False)


def transform():
    # ler os dados a partir da área de Staging.
    df_diabetes_indcators = pd.read_csv("/tmp/df_diabetes_indcators.csv")

    # Transformação da escala das features numéricas.
    df_diabetes_indcators['body_mass_index'] = df_diabetes_indcators['body_mass_index']\
        .map(lambda x: map_imc_ranges(x))

    df_diabetes_indcators['age'] = df_diabetes_indcators['age'].map(lambda x: map_age_ranges(x))

    df_diabetes_indcators['physical_illness_injury_days'] = df_diabetes_indcators['physical_illness_injury_days'] \
        .map(lambda x: map_physical_health_days(x))

    df_diabetes_indcators['income'] = df_diabetes_indcators['income'].map(lambda x: map_income_scale(x))

    df_diabetes_indcators['general_health_scale'] = df_diabetes_indcators['general_health_scale'] \
        .map(lambda x: map_health_scale(x))

    # Selecionando features de interesse.
    df_diabetes_indcators = df_diabetes_indcators.drop(['education', 'days_of_poor_mental_health',
                                                        'cholesterol_check', 'at_least_one_veggies_a_day'], axis=1)

    # Dummy das variáveis categóricas.
    variables_cat = ['income', 'age', 'general_health_scale', 'physical_illness_injury_days', 'body_mass_index']
    df_diabetes_dummy = pd.get_dummies(df_diabetes_indcators, columns=variables_cat)

    # persiste os dados transformados na área de staging.
    df_diabetes_dummy.to_csv("/tmp/df_diabetes_dummy.csv", index=False)


def load():
    # carrega os dados a partir da área de staging.
    df = pd.read_csv("/tmp/df_diabetes_dummy.csv")

    # converte os dados para o formato parquet.
    df.to_parquet("/tmp/df_diabetes_dummy.parquet", index=False)

    # carrega os dados para o Data Lake.
    client.fput_object(
        "processing",
        "df_diabetes_dummy.parquet",
        "/tmp/df_diabetes_dummy.parquet"
    )


extract_task = PythonOperator(
    task_id='extract_file_from_data_lake',
    provide_context=True,
    python_callable=extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    provide_context=True,
    python_callable=transform,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_file_to_data_lake',
    provide_context=True,
    python_callable=load,
    dag=dag
)

clean_task = BashOperator(
    task_id="clean_files_on_staging",
    bash_command="rm -f /tmp/*.csv;rm -f /tmp/*.parquet;",
    dag=dag
)

extract_task >> transform_task >> load_task >> clean_task
