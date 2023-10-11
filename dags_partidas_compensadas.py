from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
from datetime import timedelta
import os

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 22),
    
}

pd.options.display.max_columns = None


def levantar_archivo(ruta, **kwargs):
    
    ti = kwargs['ti']
    files = os.listdir(ruta)
    file = [archivo for archivo in files if archivo.endswith('.xlsx')]
    file = file[0]
    tabla_partidas =pd.read_excel(f"/{file}")  
    tabla_partidas.columns = []#Change columns names
    tabla_partidas = tabla_partidas[["CUIT"]]
    # Transform df to JSON
    tabla_partidas_json = tabla_partidas.to_json(orient='split', index=False)

    ti.xcom_push(key = "tablas_partidas" ,value=tabla_partidas_json)
    

def extraer_tabla_bq(**kwargs):
    ti = kwargs['ti']    
    client = bigquery.Client('')
    query_job = client.query(

    """
    with previo as(select distinct cuit as CUIT, mail
    from `BasesProcesadas.MailsValidos`
    where cuit != "0"),

    enumeracion as(select *, concat("Mail",row_number() over(partition by cuit))Mails 
    from previo
    order by cuit)

    select *
    from enumeracion
    PIVOT (
    max(mail)
    for Mails in ("Mail1", "Mail2", "Mail3")

    )
    """
    )
    results = query_job.result()

    data=[]
    for row in results:
        r=[]
        for f in row:
            r.append(str(f))
        data.append(r)

    tabla_bq = pd.DataFrame(data, columns=list(row.keys()))
    
    tabla_bq_json = tabla_bq.to_json(orient='split', index=False)

    ti.xcom_push(key = "tablas_bq" ,value=tabla_bq_json)

def cruce_tablas(**kwargs):
    ti = kwargs['ti']

    tabla_partidas_json = ti.xcom_pull(task_ids='Levanto_partidas',key = "tablas_partidas")
    
    tabla_partidas = pd.read_json(tabla_partidas_json, orient='split')

    tabla_bq_json = ti.xcom_pull(task_ids='traigo_mails_validos',key = "tablas_bq")
    
    tabla_bq = pd.read_json(tabla_bq_json, orient='split')


    resultado = tabla_partidas.merge(tabla_bq, on='CUIT', how='left')

    return resultado

def guardo_csv_muevo_archivos(resultado):

    files = os.listdir("")
    archivos_xlsx = [archivo for archivo in files if archivo.endswith('.xlsx')]
    archivos_xlsx = archivos_xlsx[0]
    file = archivos_xlsx.replace(".xlsx", "")
    resultado.to_excel(f"")
    os.rename = (file, "")
     
    

with DAG(dag_id='partidas_compensadas',
         description='Partidas_compensadas',
         default_args=default_args,
         schedule_interval=None) as dag:

        task1 = PythonOperator(
        task_id='Levanto_partidas',
        python_callable=levantar_archivo,
        op_args =[""],
        dag=dag,
        )

        task2 = PythonOperator(
            task_id='traigo_mails_validos',
            python_callable=extraer_tabla_bq,
            dag=dag,
        )

        task3 = PythonOperator(
            task_id='archivo_final',
            python_callable=cruce_tablas,
            provide_context=True,  
            dag=dag,
        )
        
        task4 = PythonOperator(
            task_id='Manipulacion',
            python_callable=cruce_tablas,
            op_args=[task3.output],  
            dag=dag,
        )

task1 >> task2 >> task3 >> task4
