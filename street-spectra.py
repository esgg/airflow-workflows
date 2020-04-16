# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.apache.org/tutorial.html)
"""
# [START tutorial]
from datetime import timedelta

# [START import_module]
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks import PostgresHook

import csv
import sys


# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# [END default_args]

list_dates = {}

# [START instantiate_dag]
dag = DAG(
    'street-spectra',
    default_args=default_args,
    description='Programa para tratar datos de la aplicación street-spectra',
    schedule_interval=timedelta(days=1),
)
# [END instantiate_dag]

# t1, t2 and t3 are examples of tasks created by instantiating operators
# [START basic_task]
t1 = BashOperator(
    task_id='get_data',
    bash_command='wget "https://five.epicollect.net/api/export/entries/action-street-spectra?format=csv&per_page=1000" -O /tmp/street-spectra.csv',
    retries=3,
    dag=dag,
)
t2 = BashOperator(                                                        
    task_id='unzip',                                                      
    depends_on_past=False,                                                
    bash_command='cut -d "," -f1-4,6-15 /tmp/street-spectra.csv > /tmp/street-spectra-new.csv',                                               
    retries=3,                                                            
    dag=dag,                                                              
)
def calculate_histogram(ds, **kwargs):
    reader = csv.DictReader(open("/tmp/street-spectra-new.csv"))
    for raw in reader:
        date = raw['2_Date']
        if date in list_dates:
             count = list_dates[date]
             count = count + 1
             list_dates[date] = count
        else:
             list_dates[date] = 0

    with open('/tmp/histograma.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["dia", "total"])
        keys = list_dates.keys()
        for key in keys:
            writer.writerow([str(key), list_dates[key]])

t3 = PythonOperator(
    task_id='calculate_histogram',
    provide_context=True,
    python_callable=calculate_histogram,
    dag=dag,
)

def load_data(ds, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='street_spectra_db')
    
    with open('/tmp/histograma.csv', newline='') as csvfile:
       spamreader = csv.reader(csvfile, delimiter=',')
       firstLine=True
       for row in spamreader:
          if firstLine:
             firstLine=False
          else:
             dia = row[0]
             total = row[1]
          
             row = (dia,total)                                                                                                                           
             insert_cmd = """INSERT INTO total_table                                                                                                     
                          (dia,total)                                                                                                                 
                          VALUES                                                                                                                      
                          (%s,%s);"""                                                                                                                 
             pg_hook.run(insert_cmd, parameters=row)   

t4 = PythonOperator(
     task_id='load_data',
     provide_context=True,
     python_callable=load_data,
     dag=dag)


# [END basic_task]

# [START documentation]
dag.doc_md = __doc__

t1.doc_md = """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""
# [END documentation]

# [START jinja_template]
templated_command = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}"
{% endfor %}
"""

# [END jinja_template]

t1 >> t2 >> t3 >> t4
#t1 >> [t2, t3]
# [END tutorial]
