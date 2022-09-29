# Modules
from airflow import DAG
from pathlib import Path
import dagfactory

# YAML file name and dagfactory object instantiation
dag_factory_yaml_file = 'dinamic_dag_etl_kennedy_latinoamerica.yml'
my_dir = Path(__file__).resolve().parent
dag_factory = dagfactory.DagFactory(Path(my_dir, dag_factory_yaml_file))

# Dag Factory
dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())