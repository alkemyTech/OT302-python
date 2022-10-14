# Librerias #
from airflow import DAG
import dagfactory

# Configuracion del dag dinamico
dag_fac_yml =  r'/c/Users/Jeremy/airflow/dyn_dag/dyn_dag_etl_uni_factory.py'
dag_fac = dagfactory.DAGFactory(dag_fac_yml)

# Creacion del DAG
dag_fac.clean_dags(globals())
dag_fac.generat_dags(globals())