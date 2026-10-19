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

  ## Mon 19 Oct (Checkpoint C: dbt Models and Tests)

- Done:
  - Created the dbt staging model `stg_fragments.sql` to clean the fragments stored in DuckDB.
  - Converted `order_no` from a string to an integer.
  - Added logic to remove duplicate order numbers and filter invalid fragments.
  - Added generic dbt tests to check for null values and duplicate order numbers.
  - Created the singular test `test_21_fragments.sql` to check that there are exactly 21 fragments numbered from 1 to 21.
  - Created the mart model `assembled_phrase.sql` to put the words together in the correct order.
  - Connected `dbt_build()` and `read_phrase()` to my Prefect flow.
  - Intentionally caused a dbt test to fail and saved the screenshot as `docs/dbt_tests_fail.png`.
  - Fixed the test issue and confirmed that all six dbt build steps passed.
  - Saved the passing test screenshot as `docs/dbt_tests_pass.png`.
  - Committed and pushed Checkpoint C to GitHub.

- Next:
  - Implement `submit_solution()` to send the completed phrase to the submission queue.
  - Verify that the submitted phrase is correct.

- Blocked:
  - None.


## Mon 19 Oct (Checkpoint D: Submission and Verification)

- Done:
  - Implemented `submit_solution()` to send the completed phrase to the `dp2-submit` queue.
  - Added the required message attributes: `uvaid`, `phrase`, and `platform`.
  - Confirmed that the submission returned HTTP status 200.
  - Ran `python kit/check_submission.py` to verify my answer.
  - Confirmed that my pipeline reconstructed the correct phrase.
  - Received the verified receipt code `43D5269990`.
  - Completed the full Prefect pipeline from the initial POST request to the final submission.

- Next:
  - Test the pipeline using the original message delays.
  - Save evidence of a successful full-delay run.
  - Test how the pipeline handles duplicate and malformed messages.

- Blocked:
  - None.


## Mon 19 Oct (Checkpoint E: Full-Delay Testing)

- Done:
  - Changed the environment settings to `DP2_FAST=0` to test the original message delays.
  - Restarted the Scatter API so the new settings would take effect.
  - Ran the complete Prefect pipeline with message delays ranging from 30 to 900 seconds.
  - Monitored the Prefect dashboard while the pipeline waited for the messages.
  - Successfully completed the full-delay run in 14 minutes and 22 seconds.
  - Confirmed that all six Prefect tasks completed and the submission returned HTTP status 200.
  - Saved the dashboard screenshot as `docs/full_delay_flow.png`.

- Next:
  - Enable chaos mode and test the pipeline with duplicate and malformed messages.
  - Make any necessary changes to the collection task.
  - Complete the final README documentation and self-assessed rubric.

- Blocked:
  - None.
 
 ## Afternoon Mon 19 Oct (Checkpoint F)

- Done:
  - Completed the full-delay pipeline run with `DP2_FAST=0` in 14 minutes and 22 seconds.
  - Verified that all six Prefect tasks completed successfully.
  - Saved the full-delay screenshot as `docs/full_delay_flow.png`.
  - Tested chaos mode with `DP2_CHAOS=1`, which generated 21 valid messages, 2 duplicates, and 1 malformed message.
  - Updated `collect_messages()` to count unique order numbers instead of the total number of messages received.
  - Added logic to log and delete malformed messages.
  - Confirmed that all three SQS queue counters reached zero.
  - Successfully completed two consecutive chaos-mode runs.
  - Verified that all six dbt build steps passed.
  - Ran `python kit/check_submission.py` and confirmed `2/2` correct submissions.
  - Received the verified receipt code `43D5269990`.
  - Updated the README with the completed DAG diagram, polling strategy, dbt tests, full-delay results, and chaos-mode explanation.
  - Added screenshots showing intentionally failing and passing dbt tests.

- Next:
  - Complete the self-assessment in `RUBRIC.md`.
  - Commit and push the final documentation changes to GitHub.
  - Submit the GitHub repository link and self-assessed rubric.

- Blocked:
  - None.