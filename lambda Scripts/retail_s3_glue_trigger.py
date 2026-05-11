import json
import boto3
import os

glue = boto3.client("glue")

GLUE_JOB_NAME = os.environ.get(
    "GLUE_JOB_NAME",
    "retail-scd-integration-framework-job"
)

def lambda_handler(event, context):
    print("Received event:")
    print(json.dumps(event))

    response = glue.start_job_run(
        JobName=GLUE_JOB_NAME
    )

    job_run_id = response["JobRunId"]

    print(f"Started Glue job: {GLUE_JOB_NAME}")
    print(f"Glue JobRunId: {job_run_id}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Glue job started successfully",
            "job_name": GLUE_JOB_NAME,
            "job_run_id": job_run_id
        })
    }