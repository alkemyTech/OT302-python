import dagfactory
from airflow import DAG

config_dag = "/opt/airflow/dags/moron_rio.yml"
dag_factory_moron_rio= dagfactory.DagFactory(config_dag)

# Create dependencies
dag_factory_moron_rio.clean_dags(globals())
dag_factory_moron_rio.generate_dags(globals())