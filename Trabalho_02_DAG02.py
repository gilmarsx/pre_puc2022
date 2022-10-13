import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

URL = "https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv"

default_args = {
    'owner': "Gilmar",
    "depends_on_past": False,
    'start_date': datetime(2022, 10, 9)
}


@dag(default_args=default_args, schedule_interval=None, catchup=False, tags=['DAG2'])
def trabalho02_dag02():

    @task
    def tab_unica():
        NOME_DO_ARQUIVO = "/tmp/tabela_unica.csv"
        df = pd.read_csv(NOME_DO_ARQUIVO, sep=";")
        df.to_csv(NOME_DO_ARQUIVO, index=False, header=True, sep=";")
        return NOME_DO_ARQUIVO

    @task
    def tab_indicadores(NOME_DO_ARQUIVO):
        TABELA_INDICADORES = "/tmp/resultados.csv"
        df = pd.read_csv(NOME_DO_ARQUIVO, sep=";")
        res = df.groupby(['Sex']).agg(
            {"Fare": "mean", "sibsp_parch": "mean"}).reset_index()
        print(res)
        res.to_csv(TABELA_INDICADORES, index=False, sep=";")
        return TABELA_INDICADORES

    start = DummyOperator(task_id="inicio")
    fim = DummyOperator(task_id="fim")

    df_unica = tab_unica()
    df_indicadores = tab_indicadores(df_unica)

    start >> df_unica >> df_indicadores >> fim


execucao = trabalho02_dag02()
