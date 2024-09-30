from dagster import ScheduleDefinition, repository
from app.dagster_job import scheduled_job

# Definir o agendamento para rodar todos os dias às 21h
schedule = ScheduleDefinition(
    job=scheduled_job,
    cron_schedule="0 21 * * *"  # Todos os dias às 21h
)

@repository
def dagster_repo():
    return [scheduled_job, schedule]
