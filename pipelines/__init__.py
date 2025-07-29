from dagster import Definitions 
from .dagster_pipeline import summary_job, summary_schedule

defs = Definitions(jobs=[summary_job], schedules=[summary_schedule])