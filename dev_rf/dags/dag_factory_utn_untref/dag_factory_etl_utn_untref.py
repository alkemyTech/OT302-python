from airflow import DAG
import dagfactory

dag_factory = dagfactory.DagFactory("/opt/airflow/dags/config_file_etl_utn_untref.yml")

# Clean old DAGs that are not on YAML config but were auto-generated through dag-factory
dag_factory.clean_dags(globals())

# Generates DAGs from YAML config
dag_factory.generate_dags(globals())
