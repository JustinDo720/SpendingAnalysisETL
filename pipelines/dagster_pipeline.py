from dagster import job, op, ScheduleDefinition

# Operation 
@op
def summary_operation():
    # Transforming summary 
    from transformer import transform_summary
    transform_summary()

# Job 
@job 
def summary_job():
    # Running the Summary 
    summary_operation()

# Crontab scheduling 
summary_schedule = ScheduleDefinition(
    job=summary_job,
    cron_schedule="*/5 * * * *", # every 5 minutes 
    execution_timezone='US/Eastern',
    name='summary_job_schedule'
)