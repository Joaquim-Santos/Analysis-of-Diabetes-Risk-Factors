import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from minio import Minio


DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 1, 1),
}

dag = DAG('etl_features_selection',
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
    obj = client.get_object(
        "landing",
        "diabetes_binary_health_indicators_BRFSS2015.csv",
    )
    df = pd.read_csv(obj)

    # persiste o dataset na área de Staging.
    df.to_csv("/tmp/df_diabetes_indcators.csv", index=False)


def transform():
    # ler os dados a partir da área de Staging.
    df_diabetes_indcators = pd.read_csv("/tmp/df_diabetes_indcators.csv")

    # Renomeando atributos
    df_diabetes_indcators = df_diabetes_indcators.rename(
        columns={'Diabetes_binary': 'diabetes',
                 'HighBP': 'high_blood_preassure',
                 'HighChol': 'high_cholesterol',
                 'CholCheck': 'cholesterol_check',
                 'BMI': 'body_mass_index',
                 'Smoker': 'smoker',
                 'Stroke': 'avc',
                 'HeartDiseaseorAttack': 'heart_diseaseor_attack',
                 'PhysActivity': 'physical_activity_in_past_30_days',
                 'Fruits': 'at_least_one_fruit_a_day',
                 'Veggies': 'at_least_one_veggies_a_day',
                 'HvyAlcoholConsump': 'high_consumption_of_alcohol',
                 'AnyHealthcare': 'any_healthcare',
                 'NoDocbcCost': 'no_doctor_because_cost',
                 'GenHlth': 'general_health_scale',
                 'MentHlth': 'days_of_poor_mental_health',
                 'PhysHlth': 'physical_illness_injury_days',
                 'DiffWalk': 'serious_difficulty_walking',
                 'Sex': 'sex',
                 'Age': 'age',
                 'Education': 'education',
                 'Income': 'income'
                 }
    )

    # Alterando os tipos de dados.
    columns = list(df_diabetes_indcators.columns)
    columns.remove('body_mass_index')
    for column in columns:
        df_diabetes_indcators[column] = df_diabetes_indcators[column].astype(int)

    # Removendo dados duplicados (valores iguais em todas as colunas).
    df_diabetes_indcators.drop_duplicates(subset=None, keep='first', inplace=True)

    # persiste os dados transformados na área de staging.
    df_diabetes_indcators[["high_blood_preassure", "high_cholesterol", "body_mass_index", "age",
                           "physical_activity_in_past_30_days", "physical_illness_injury_days",
                           "education", "income", "serious_difficulty_walking", "heart_diseaseor_attack",
                           "days_of_poor_mental_health", "general_health_scale", "avc", "cholesterol_check",
                           "smoker", "at_least_one_veggies_a_day", "high_consumption_of_alcohol", "diabetes"]]\
        .to_csv("/tmp/df_diabetes_indcators.csv", index=False)


def load():
    # carrega os dados a partir da área de staging.
    df = pd.read_csv("/tmp/df_diabetes_indcators.csv")

    # converte os dados para o formato parquet.
    df.to_parquet("/tmp/df_diabetes_indcators.parquet", index=False)

    # carrega os dados para o Data Lake.
    client.fput_object(
        "processing",
        "df_diabetes_indcators.parquet",
        "/tmp/df_diabetes_indcators.parquet"
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
