
""" Dinamic dags
"""

from airflow import DAG
import dagfactory

dag_factory = dagfactory.DagFactory("/opt/airflow/dags/wl_jb_config_file.yml")

dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())