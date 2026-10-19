# Progress log

Add a new entry at the end of every session (and whenever you stop working).

## Mon 05 Oct (Lesson 9)
- Done:
  - Set up Python 3.12.3 virtual environment and installed project requirements.
  - Created `.env` and set my UVA computing ID.
  - Started the Moto SQS emulator and scatter API.
  - Ran `python kit/smoke_test.py` successfully and got `Emulator OK`.
  - Started the Prefect server and verified the API is working.
  - Reviewed Checkpoint A and the starter `prefect-flow.py`.

- Next:
  - Implement `populate_queue()`, `get_counts()`, and `monitor_queue()` for Checkpoint A.
  - Run the Prefect flow and verify the queue counters.
  - Confirm the completed flow appears in the Prefect dashboard.
  - Create the DAG sketch and explain the polling strategy in the README.

- Blocked:
  - None.


  ## Thu 08 Oct (Lesson 10)

- Done:
  - Implemented `populate_queue()`, `get_counts()`, and `monitor_queue()`.
  - Successfully ran the Prefect flow and monitored all 21 delayed SQS messages.
  - Confirmed the completed flow in the Prefect dashboard.
  - Created the DAG diagram and saved the Checkpoint A screenshot in `docs/`.
  - Updated the README with the polling strategy.
  - Committed and pushed Checkpoint A to GitHub.

- Next:
  - Implement `collect_messages()` to store SQS fragments in DuckDB.
  - Connect message collection to the Prefect flow.
  - Verify that all 21 fragments are stored.

- Blocked:
  - None.

## Mon 19 Oct (Checkpoint B)

- Done:
  - Restarted the Moto SQS emulator, Scatter API, and Prefect server.
  - Successfully ran `python kit/smoke_test.py`.
  - Implemented `collect_messages()` using boto3 and DuckDB.
  - Created the `raw.fragments` table with `order_no`, `word`, and `received_at`.
  - Added logic to receive SQS messages, save them in DuckDB, and delete them after saving.
  - Fixed duplicate Scatter API calls so the flow populates the queue only once.
  - Successfully completed the FAST-mode Prefect flow (`benign-dogfish`).
  - Verified 21 stored fragments, 21 unique order numbers, and 21 non-null words.
  - Saved the Prefect screenshot as `docs/checkpoint_b_flow.png`.

- Next:
  - Implement dbt staging and mart models.
  - Add dbt tests to validate the fragments and reconstructed phrase.
  - Complete the submission task and remaining robustness tests.

- Blocked:
  - None.
  
