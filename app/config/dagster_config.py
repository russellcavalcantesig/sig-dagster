# app/config/dagster_config.py

import os
from pathlib import Path

def setup_dagster_config():
    """Configure Dagster home directory and create necessary configuration files."""
    
    # Configurar DAGSTER_HOME
    DAGSTER_HOME = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "dagster_home")
    os.environ["DAGSTER_HOME"] = DAGSTER_HOME

    # Garantir que o diretório DAGSTER_HOME existe
    Path(DAGSTER_HOME).mkdir(parents=True, exist_ok=True)

    # Criar arquivo dagster.yaml no DAGSTER_HOME se não existir
    dagster_yaml_path = os.path.join(DAGSTER_HOME, "dagster.yaml")
    if not os.path.exists(dagster_yaml_path):
        with open(dagster_yaml_path, "w") as f:
            f.write("""
run_storage:
  module: dagster_postgres.run_storage
  class: PostgresRunStorage
  config:
    postgres_db:
      hostname: localhost
      username: postgres
      password: "123456"
      db_name: sig
      port: 5432

event_log_storage:
  module: dagster_postgres.event_log
  class: PostgresEventLogStorage
  config:
    postgres_db:
      hostname: localhost
      username: postgres
      password: "123456"
      db_name: sig
      port: 5432

schedule_storage:
  module: dagster_postgres.schedule_storage
  class: PostgresScheduleStorage
  config:
    postgres_db:
      hostname: localhost
      username: postgres
      password: "123456"
      db_name: sig
      port: 5432
""")
    
    return DAGSTER_HOME