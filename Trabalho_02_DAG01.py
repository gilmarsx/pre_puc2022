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


@dag(default_args=default_args, schedule_interval='@once', catchup=False, tags=['DAG1'])
def trabalho02_dag01():

    @task
    def cap_dados():
        NOME_DO_ARQUIVO = "/tmp/titanic.csv"
        df = pd.read_csv(URL, sep=';')
        df.to_csv(NOME_DO_ARQUIVO, index=False, sep=";")
        return NOME_DO_ARQUIVO

    @task
    def ind_passageiros(NOME_DO_ARQUIVO):
        NOME_TABELA = "/tmp/passageiros_por_sexo_classe.csv"
        df = pd.read_csv(NOME_DO_ARQUIVO, sep=";")
        res = df.groupby(['Sex', 'Pclass']).agg(
            {"PassengerId": "count"}).reset_index()
        print(res)
        res.to_csv(NOME_TABELA, index=False, sep=";")
        return NOME_TABELA

    @task
    def ind_ticket(NOME_DO_ARQUIVO):
        NOME_TABELA2 = "/tmp/ticket_por_sexo_classe.csv"
        df = pd.read_csv(NOME_DO_ARQUIVO, sep=";")
        res = df.groupby(['Sex', 'Pclass']).agg({"Fare": "mean"}).reset_index()
        print(res)
        res.to_csv(NOME_TABELA2, index=False, sep=";")
        return NOME_TABELA2

    @task
    def ind_sibsp_parch(NOME_DO_ARQUIVO):
        NOME_TABELA3 = "/tmp/sibsp_parch_sexo_classe.csv"
        df = pd.read_csv(NOME_DO_ARQUIVO, sep=";")
        df['sibsp_parch'] = df['SibSp'] + df['Parch']
        res = df.groupby(['Sex', 'Pclass']).agg(
            {"sibsp_parch": "count"}).reset_index()
        print(res)
        res.to_csv(NOME_TABELA3, index=False, sep=";")
        return NOME_TABELA3

    @task
    def get_together(NOME_TABELA, NOME_TABELA2, NOME_TABELA3):
        TABELA_UNICA = "/tmp/tabela_unica.csv"
        df = pd.read_csv(NOME_TABELA, sep=";")
        df1 = pd.read_csv(NOME_TABELA2, sep=";")
        df2 = pd.read_csv(NOME_TABELA3, sep=";")

        df3 = df.merge(df1, on=['Sex', 'Pclass'], how='inner')
        df4 = df2.merge(df3, on=['Sex', 'Pclass'], how='inner')
        print(df4)
        df4.to_csv(TABELA_UNICA, index=False, sep=";")
        return TABELA_UNICA

    inicio = DummyOperator(task_id="inicio")
    fim = DummyOperator(task_id="fim")

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def end():
        print("Terminou")
    triggerdag = TriggerDagRunOperator(
        task_id="trigga_trabalho_02_ex02",
        trigger_dag_id="trabalho02_dag02")

    df = cap_dados()
    indicador = ind_passageiros(df)
    ticket = ind_ticket(df)
    sibsp_parch = ind_sibsp_parch(df)
    tab_final = get_together(indicador, ticket, sibsp_parch)

    inicio >> df >> [indicador, ticket,
                     sibsp_parch] >> tab_final >> fim >> triggerdag


execucao = trabalho02_dag01()
