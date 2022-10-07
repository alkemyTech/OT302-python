import pathlib
work_path = pathlib.Path().resolve()

from airflow import DAG
import dagfactory
print(work_path)

dag_factory = dagfactory.DagFactory(f"{work_path}/config_file.yml")

dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())
