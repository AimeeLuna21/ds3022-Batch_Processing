"""DS 3022 Data Project 2: Prefect flow.

Your pipeline must, without human intervention:
  1. POST to the scatter API to populate your queue (once per run, never inside a retry loop)
  2. Monitor the queue (get_queue_attributes) and collect all 21 messages
  3. Land each fragment in DuckDB (raw.fragments) BEFORE deleting its message
  4. Run dbt: tests are the quality gate, the mart model holds the phrase
  5. Submit the phrase to the 'dp2-submit' queue and log the HTTP status code

Run:   python prefect-flow.py
Then:  python kit/check_submission.py

Every task already has a header. Checkpoint A (Lesson 9): fill in populate_queue,
get_counts and monitor_queue. Lesson 10: collect_messages, dbt_build, read_phrase
and submit_solution. The tasks not called by the flow yet cannot fail, so you can
run the file at any point. The skeleton is a suggestion: rename, split or add
tasks as your design needs, and keep your DAG sketch in step with them.
Use Prefect's logger (get_run_logger) for logging. Never hard-code your
computing ID or the endpoint: read them from the environment (.env).
"""
import os
import time
from venv import logger

import boto3
import duckdb
from fastapi import logger
import requests
import subprocess
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

load_dotenv()

UVA_ID = os.environ["UVA_ID"]
SCATTER_URL = f"http://127.0.0.1:{os.environ.get('SCATTER_PORT', '8000')}/api/scatter/{UVA_ID}"
DUCKDB_PATH = os.environ.get("DP2_DUCKDB", "dp2.duckdb")

sqs = boto3.client("sqs")  # the endpoint comes from AWS_ENDPOINT_URL_SQS


@task
def populate_queue() -> str:
    """POST to the scatter API and return your queue URL."""
    # Get Prefect's logger so we can track what happens during the task
    logger = get_run_logger()

    # Send a POST request to the Scatter API to create our puzzle messages
    # timeout=30 means stop waiting if the API does not respond within 30 seconds
    resp = requests.post(SCATTER_URL, timeout=30)

    # Check whether the API request was successful
    # If it failed (e.g., HTTP 500), Python raises an error
    resp.raise_for_status()

    # Convert the API's JSON response into a Python dictionary
    payload = resp.json()

    # Print the API response in the Prefect logs
    logger.info("Scatter API answered: %s", payload)

    # Extract the SQS queue URL and return it to the Prefect flow
    return payload["sqs_url"]


def get_counts(queue_url: str) -> dict:

    """basically get 3 SQS message counters."""

    #Ask SQS for the three queue attributes: ApproximateNumberOfMessages, ApproximateNumberOfMessagesNotVisible, ApproximateNumberOfMessagesDelayed
    response = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed",
        ],
    )

    # Extract the attributes from the response
    attributes = response["Attributes"]

    # Convert the counts from strings to integers
    return {
        "visible": int(attributes["ApproximateNumberOfMessages"]),
        "in_flight": int(attributes["ApproximateNumberOfMessagesNotVisible"]),
        "delayed": int(attributes["ApproximateNumberOfMessagesDelayed"])
    }
    

@task
def monitor_queue(queue_url: str, poll_s: int = 5, timeout_s: int = 1200) -> dict:
    """Monitor the SQS queue until all delayed messages are released."""

    # Get Prefect's logger to record the queue counts
    logger = get_run_logger()

    # Calculate the time when monitoring must stop
    # time.monotonic() tracks elapsed time
    # timeout_s = 1200 seconds (20 minutes)
    deadline = time.monotonic() + timeout_s

    # Keep checking the queue until the deadline is reached
    while time.monotonic() < deadline:

        # Call our get_counts() helper to retrieve the three counters
        c = get_counts(queue_url)

        # Log the number of visible, in-flight, and delayed messages
        logger.info(
            "visible=%d  in_flight=%d  delayed=%d",
            c["visible"],
            c["in_flight"],
            c["delayed"]
        )

        # If no messages remain delayed, stop monitoring
        if c["delayed"] == 0:
            logger.info("All messages released")
            return c

        # Wait 5 seconds (by default) before checking again
        time.sleep(poll_s)

    # If reach the deadline, stop the task with an error
    raise TimeoutError(f"Messages still delayed after {timeout_s} s")

def stored_count(con) -> int:
    """Count how many unique fragments are stored in DuckDB."""

    return con.execute(
        "SELECT COUNT(DISTINCT order_no) FROM raw.fragments"
    ).fetchone()[0]

@task
def collect_messages(queue_url: str, expected: int = 21) -> int:
    """Collect all messages, store valid fragments, and empty the queue."""

    logger = get_run_logger()

    # Connect to DuckDB
    con = duckdb.connect(DUCKDB_PATH)

    try:
        # Create the table if it does not already exist
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")

        con.execute("""
            CREATE TABLE IF NOT EXISTS raw.fragments (
                order_no VARCHAR,
                word VARCHAR,
                received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Clear data from previous pipeline runs
        con.execute("DELETE FROM raw.fragments")
        con.commit()

        # Give the collection task 20 minutes to finish
        deadline = time.monotonic() + 1200

        # Continue until we have 21 unique fragments and an empty queue
        while time.monotonic() < deadline:

            # Count unique fragments already stored in DuckDB
            stored = stored_count(con)

            # Check the three SQS queue counters
            counts = get_counts(queue_url)

            logger.info(
                "Stored %d/%d | visible=%d in_flight=%d delayed=%d",
                stored,
                expected,
                counts["visible"],
                counts["in_flight"],
                counts["delayed"]
            )

            # Only finish when all fragments are stored
            # AND there are no messages left in the queue
            if stored >= expected and sum(counts.values()) == 0:
                logger.info("All fragments collected and queue is empty")
                return stored

            # Receive up to 10 messages from SQS
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                MessageAttributeNames=["All"],
                WaitTimeSeconds=10
            )

            # An empty response is normal when no messages are available
            for message in response.get("Messages", []):

                # Get the message attributes safely
                attributes = message.get("MessageAttributes", {})

                order_no = attributes.get(
                    "order_no", {}
                ).get("StringValue")

                word = attributes.get(
                    "word", {}
                ).get("StringValue")

                # If either attribute is missing, log the problem
                if not order_no or not word:
                    logger.warning(
                        "Malformed message skipped: %s",
                        attributes
                    )

                else:
                    # Save valid fragments to DuckDB
                    # Duplicates are allowed here; dbt removes them later
                    con.execute(
                        """
                        INSERT INTO raw.fragments (order_no, word)
                        VALUES (?, ?)
                        """,
                        [order_no, word]
                    )

                    # Save the data before deleting the SQS message
                    con.commit()

                    logger.info(
                        "Stored fragment with order_no=%s",
                        order_no
                    )

                # Delete both valid and malformed messages
                # This prevents messages from remaining in SQS
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message["ReceiptHandle"]
                )

        # If 20 minutes pass without completing, stop with an error
        stored = stored_count(con)
        raise TimeoutError(
            f"Only collected {stored}/{expected} unique fragments "
            "before the timeout"
        )

    finally:
        # Always close the DuckDB connection
        con.close()

@task
def dbt_build() -> None:
    """Build dbt models and run all quality tests."""

    # Get the Prefect logger to record what happens during this task
    logger = get_run_logger()

    # Let us know that dbt is starting
    logger.info("Starting dbt build and quality tests")

    # Run dbt build to create the models and check all the tests
    result = subprocess.run(
        ["dbt", "build", "--project-dir", "dbt", "--profiles-dir", "dbt"],
        capture_output=True,  # Save the command's output
        text=True              # Return the output as readable text
    )

    # Show the dbt results in the Prefect logs
    logger.info(result.stdout)

    # If dbt fails, stop the pipeline so we don't submit incorrect data
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError("dbt build failed: quality checks did not pass")

    # Confirm that the models and tests completed successfully
    logger.info("dbt build completed successfully")



@task
def read_phrase() -> str:
    """Read the reconstructed phrase from DuckDB."""

    # Get the Prefect logger
    logger = get_run_logger()

    # Connect to the DuckDB database
    con = duckdb.connect(DUCKDB_PATH, read_only=True)

    try:
        # Read the phrase created by our dbt assembled_phrase model
        result = con.execute(
            "SELECT phrase FROM main.assembled_phrase"
        ).fetchone()

        # Stop the pipeline if the phrase is missing or empty
        if result is None or not result[0]:
            raise ValueError("No assembled phrase found in DuckDB")

        # Save the reconstructed phrase in a Python variable
        phrase = result[0]

        # Record the phrase in the Prefect logs
        logger.info("Reconstructed phrase: %s", phrase)

        # Return the phrase for the submission task
        return phrase

    finally:
        # Always close the database connection
        con.close()


@task
def submit_solution(phrase: str, platform: str = "prefect") -> int:
    """Submit the reconstructed phrase to the dp2-submit queue."""

    # Get the Prefect logger
    logger = get_run_logger()

    # Find the submission queue
    queue_url = sqs.get_queue_url(QueueName="dp2-submit")["QueueUrl"]

    # Send the completed phrase with the required message attributes
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody="dp2 solution",
        MessageAttributes={
            "uvaid": {
                "DataType": "String",
                "StringValue": os.environ["UVA_ID"]
            },
            "phrase": {
                "DataType": "String",
                "StringValue": phrase
            },
            "platform": {
                "DataType": "String",
                "StringValue": platform
            },
        },
    )

    # Get the HTTP status code from the SQS response
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]

    # Record the result in the Prefect logs
    logger.info("Submission HTTP status: %s", status_code)

    # Stop the pipeline if submission was unsuccessful
    if status_code != 200:
        raise RuntimeError(f"Submission failed with HTTP status {status_code}")

    # Return the HTTP status code
    return status_code


@flow(name="dp2-pipeline", log_prints=True)
def dp2_pipeline():

    # Step 1: Generate the 21 SQS messages (once per run)
    queue_url = populate_queue()

    # Step 2: Wait until all delayed messages are released
    monitor_queue(queue_url)

    # Step 3: Collect and store the 21 messages in DuckDB
    stored = collect_messages(queue_url)

    # Log how many fragments were collected
    print(f"Total fragments stored: {stored}")

    # Step 4: Build the dbt models and run all quality tests
    # If a test fails, the pipeline stops here
    dbt_build()

    # Step 5: Read the reconstructed phrase from DuckDB
    phrase = read_phrase()

    # Show the completed phrase in the Prefect logs
    print(f"Reconstructed phrase: {phrase}")

    # Step 6: Submit the reconstructed phrase to the dp2-submit queue
    # This only runs if all dbt quality tests passed
    status_code = submit_solution(phrase)

    # Log the submission result
    print(f"Submission completed with HTTP status: {status_code}")



if __name__ == "__main__":
    dp2_pipeline()
