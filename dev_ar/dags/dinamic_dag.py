# Modules
from airflow import DAG
import dagfactory

# YAML file name and dagfactory object instantiation
dag_factory_yaml_file = 'dinamic_dag_etl_kennedy_latinoamerica.yml'
dag_factory = dagfactory.DagFactory(dag_factory_yaml_file)

# Dag Factory
dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())