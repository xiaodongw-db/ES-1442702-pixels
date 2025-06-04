# Databricks notebook source
# MAGIC %md 
# MAGIC # Solution Accelerator Deployment
# MAGIC This notebook sets up clusters, a multi-task job (workflow), and applies ACLs.

# COMMAND ----------

# DBTITLE 0,Install packages with SDK version check
# MAGIC %pip install --quiet git+https://github.com/databricks-academy/dbacademy@v1.0.13 \
# MAGIC git+https://github.com/databricks-industry-solutions/notebook-solution-companion@serverless \
# MAGIC databricks-sdk==0.37 urllib3==1.26.20 --upgrade --force-reinstall

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from solacc.companion import NotebookSolutionCompanion
from databricks.sdk import WorkspaceClient

from databricks.sdk.service.jobs import JobPermissionLevel, JobAccessControlRequest
CAN_MANAGE = JobPermissionLevel.CAN_MANAGE

# COMMAND ----------

# Define job configuration with ACLs
job_json = {
    "timeout_seconds": 7200,
    "max_concurrent_runs": 1,
    "tags": {
        "usage": "solacc_testing",
        "group": "HLS",
        "accelerator": "pixels"
    },
    "tasks": [
        {
            "notebook_task": {
                "notebook_path": "EXECUTE"
            },
            "task_key": "EXECUTE"
        }
    ],
    "access_control_list": [
        JobAccessControlRequest(
            group_name="users",
            permission_level=CAN_MANAGE
        ).as_dict()
    ]
}

# COMMAND ----------

# Set up widget to control job execution
dbutils.widgets.dropdown("run_job", "True", ["True", "False"])
run_job = dbutils.widgets.get("run_job") == "True"

# COMMAND ----------

# Deploy the job and get job_id
print("Deploying job...")
companion = NotebookSolutionCompanion()
job_id = companion.deploy_compute(job_json, run_job=run_job)
print(f"Job deployed with job_id: {job_id}")

# Verify permissions
if hasattr(companion, 'w') and isinstance(companion.w, WorkspaceClient):
    try:
        print("Current job permissions:")
        print(companion.w.jobs.get_permission_levels(job_id=job_id))
    except Exception as e:
        print(f"Permission check failed: {str(e)}")
