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

import boto3
import duckdb
import requests
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


@task
def collect_messages(queue_url: str, expected: int = 21) -> int:
    """Receive, store in DuckDB, then delete all expected SQS messages."""

    logger = get_run_logger()

    # Connect to DuckDB
    con = duckdb.connect(DUCKDB_PATH)

    try:
        # Create the raw schema and fragments table
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")

        con.execute("""
            CREATE TABLE IF NOT EXISTS raw.fragments (
                order_no VARCHAR,
                word VARCHAR,
                received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Clear fragments from previous runs
        con.execute("DELETE FROM raw.fragments")

        # Keep track of how many messages we've stored
        stored = 0

        # Stop if messages never arrive
        deadline = time.monotonic() + 1200

        # Keep collecting until all 21 fragments are stored
        while stored < expected:

            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Only collected {stored} of {expected} fragments"
                )

            # Request up to 10 messages from SQS
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                MessageAttributeNames=["All"],
                WaitTimeSeconds=5
            )

            # SQS may return an empty response
            messages = response.get("Messages", [])

            if not messages:
                logger.info("No messages available yet; checking again")
                continue

            for message in messages:
                 # Stop processing once we have all 21 fragments
                if stored >= expected:
                    break

                # Extract attributes safely
                attributes = message.get("MessageAttributes", {})

                order_no = attributes.get(
                    "order_no", {}
                ).get("StringValue")

                word = attributes.get(
                    "word", {}
                ).get("StringValue")

                # Save the message to DuckDB first
                con.execute(
                    """
                    INSERT INTO raw.fragments (order_no, word)
                    VALUES (?, ?)
                    """,
                    [order_no, word]
                )

                # Ensure the database write is committed
                con.commit()


                # Delete only after DuckDB successfully stores it
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message["ReceiptHandle"]
                )

                stored += 1

                logger.info(
                    "Stored and deleted fragment %d/%d: order_no=%s",
                    stored,
                    expected,
                    order_no
                )

        logger.info("Successfully collected %d fragments", stored)

        return stored

    finally:
        # Always close the database connection
        con.close()



@task
def dbt_build() -> None:
    """Run `dbt build --project-dir dbt --profiles-dir dbt`; raise if anything fails."""
    # TODO (Checkpoint C)
    raise NotImplementedError


@task
def read_phrase() -> str:
    """Read the phrase from the dbt mart model in DuckDB."""
    # TODO (Checkpoint C)
    raise NotImplementedError


@task
def submit_solution(phrase: str, platform: str = "prefect") -> int:
    """Send to dp2-submit with uvaid / phrase / platform attributes. Return the HTTP status code."""
    # TODO (Checkpoint D)
    raise NotImplementedError


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

    # Later: dbt_build, read_phrase, submit_solution
    
if __name__ == "__main__":
    dp2_pipeline()
