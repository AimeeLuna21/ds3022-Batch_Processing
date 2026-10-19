# DS 3022 Data Project 2 (Fall 2026): The SQS Puzzle

## Project Overview

For this project, I created a data pipeline using Prefect, SQS, DuckDB, and dbt. The goal was to collect 21 messages that each contained a word and an order number, then put the words back together to form a complete phrase.

The main challenge was that the messages didn't all arrive at once. Some were delayed, so I had to make sure my pipeline waited for all of them before putting the phrase together. I also needed to store the messages in DuckDB and use dbt to check that I had all 21 words in the correct order.

Once I got the regular pipeline working, I tested it with longer delays and chaos mode, where extra duplicate and malformed messages were added. I had to make some changes to my collection task so it could handle those messages without affecting the final answer.

## 1. Pipeline Architecture

I used Prefect to organize my pipeline into six tasks:

1. **populate_queue:** Sends an HTTP POST request to the Scatter API to generate the messages.
2. **monitor_queue:** Checks the SQS queue to see when the delayed messages are ready.
3. **collect_messages:** Receives the messages, saves the fragments in DuckDB, and deletes the messages from SQS.
4. **dbt_build:** Runs the dbt models and tests to clean and check the data.
5. **read_phrase:** Reads the completed phrase from DuckDB.
6. **submit_solution:** Submits the phrase to the `dp2-submit` queue and checks that the request was successful.

### Prefect Flow DAG

![Completed Prefect Flow DAG](docs/DAG_diagram.png)

I organized the tasks in this order because each one depends on the previous task. For example, I cannot run the dbt tests until the messages have been collected, and I do not want to submit the phrase until the tests confirm that all the fragments are there.

## 2. How to Run My Pipeline

### Initial Setup

I completed this project using GitHub Codespaces. I used Moto to simulate AWS SQS locally, so I did not need a real AWS account.

To set up the project from a fresh clone, follow the instructions in [SETUP.md](SETUP.md). This includes creating a virtual environment, installing the requirements, and setting up the `.env` file.

My `.env` file contains these settings:

```env
UVA_ID=xdd9uz
AWS_ENDPOINT_URL_SQS=http://127.0.0.1:9324
AWS_DEFAULT_REGION=us-east-1
SCATTER_PORT=8000
DP2_FAST=1
DP2_CHAOS=0
DP2_DUCKDB=dp2.duckdb
AWS_ACCESS_KEY_ID=testing
AWS_SECRET_ACCESS_KEY=testing
```

The AWS credentials are just dummy values because I am using the local emulator. I did not upload my `.env` file to GitHub.

### Starting the Services

I used four terminals in Codespaces to run everything.

**Terminal 1: Moto SQS Emulator**

```bash
source .venv/bin/activate
moto_server -p 9324
```

**Terminal 2: Scatter API**

```bash
source .venv/bin/activate
python kit/scatter_api.py
```

**Terminal 3: Running My Pipeline**

```bash
source .venv/bin/activate
```

Before running the pipeline, I checked that the emulator was working:

```bash
python kit/smoke_test.py
```

The output should say:

```text
Emulator OK
```

Once the other services are running, I start my pipeline with:

```bash
python prefect-flow.py
```

**Terminal 4: Prefect Server**

```bash
source .venv/bin/activate
prefect server start
```

I also configured Prefect to connect to the local server:

```bash
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```

This configuration needs to be set before running the pipeline in Terminal 3.

The Prefect dashboard can be opened at:

http://127.0.0.1:4200

After starting the Moto emulator, Scatter API, and Prefect server, I can run the entire pipeline using one command in Terminal 3.

### Different Testing Modes

I used three different settings while testing my pipeline.

**Fast Mode**

```env
DP2_FAST=1
DP2_CHAOS=0
```

This makes the messages arrive faster, which was useful when I was still working on my code.

**Full-Delay Mode**

```env
DP2_FAST=0
DP2_CHAOS=0
```

This uses the original message delays of 30 to 900 seconds.

**Chaos Mode**

```env
DP2_FAST=1
DP2_CHAOS=1
```

This adds duplicate and malformed messages to test whether my pipeline can handle unexpected data.

After changing these settings in `.env`, I restart the Scatter API before running the pipeline again.

## 3. Polling Strategy

One of the first things I had to figure out was how to deal with the messages arriving at different times.

I decided to use polling, which means my pipeline checks the queue repeatedly instead of assuming that all the messages will be available right away.

My `monitor_queue` task uses `get_queue_attributes()` to check three values:

- **ApproximateNumberOfMessages:** Messages that are visible and ready to receive.
- **ApproximateNumberOfMessagesNotVisible:** Messages that have been received but have not been deleted.
- **ApproximateNumberOfMessagesDelayed:** Messages that are still waiting for their delay to finish.

I made the monitoring task check these values every five seconds until there were no delayed messages left.

I chose five seconds because I wanted the pipeline to check regularly without making requests constantly. I could have used a fixed waiting time, but I did not think that was the best approach because the delays are random. If I waited too little, some messages might not be ready, and if I waited too long, I would waste time.

When collecting the messages, I also used long polling with:

```python
WaitTimeSeconds=10
```

This lets SQS wait briefly for messages instead of immediately returning an empty response.

I also learned that SQS does not always return a `Messages` key when there are no messages available. To prevent my code from crashing, I used:

```python
response.get("Messages", [])
```

This returns an empty list if no messages were received.

Finally, I added a timeout of 1,200 seconds. If the messages never arrive, the pipeline raises an error instead of running forever.

## 4. Collecting Messages and Storing Them in DuckDB

After the monitoring task finishes, my pipeline starts collecting the messages.

Each message has two important attributes:

- `order_no`: Tells me where the word belongs in the phrase.
- `word`: Contains the actual word.

To access these attributes, I had to request them using:

```python
MessageAttributeNames=["All"]
```

I stored the fragments in a DuckDB table called `raw.fragments`. The table contains the order number, word, and time the message was received.

### Why I Store Messages Before Deleting Them

One important part of this project was making sure that I saved each message before deleting it from SQS.

My collection task follows these steps:

1. Receive the message from SQS.
2. Read the order number and word.
3. Insert the fragment into DuckDB.
4. Commit the database transaction.
5. Delete the message from SQS using its receipt handle.

I did it in this order because if I deleted a message before saving it and something went wrong, I could lose that word permanently.

There is also a chance that the program could crash after saving a message but before deleting it. If that happens, SQS might deliver the same message again.

This would create duplicate rows in DuckDB, but my dbt staging model handles this by keeping only one row for each order number.

This helps my pipeline handle duplicates, although it does not mean the entire flow can recover from every possible crash. My collection task clears the raw table when a new run starts, so a restarted run begins collecting fragments again.

I also made the collection task check that I had 21 distinct order numbers and that all three queue counters were zero before moving on.

## 5. dbt Models and Quality Checks

I used dbt to make sure that the data was correct before putting the final phrase together.

I created a staging model, a mart model, and tests that check whether all 21 fragments were collected.

### Staging Model: `stg_fragments.sql`

My staging model does a few things:

- Converts `order_no` from a string to an integer.
- Filters out invalid fragments.
- Removes duplicate order numbers.
- Prepares the data for the final phrase.

The duplicate handling became especially important when I started testing chaos mode.

### Generic Tests

I added generic tests to check for missing values and duplicate order numbers.

For example, I used `not_null` and `unique` tests to make sure the staging data did not contain missing or repeated order numbers.

### Singular Test: `test_21_fragments.sql`

I also created a singular test to check that there were exactly 21 fragments and that their order numbers went from 1 to 21 without any gaps.

This is important because even if I collected several messages successfully, that does not necessarily mean I collected every word.

If a word is missing, the test fails.

### Mart Model: `assembled_phrase.sql`

Once the fragments pass the tests, my mart model puts the words in the correct order and combines them into one phrase.

My Prefect flow runs:

```bash
dbt build --project-dir dbt --profiles-dir dbt
```

If dbt fails, my Prefect task raises an error, so the pipeline does not continue to the submission step.

### Testing the Quality Gate

To make sure my tests actually worked, I intentionally caused a test to fail.

**Failing dbt test:**

![Failing dbt tests](docs/dbt_tests_fail.png)

After correcting the issue, I ran the dbt build again and all six build steps passed.

**Passing dbt tests:**

![Passing dbt tests](docs/dbt_tests_pass.png)

This showed me why the dbt tests were important. Just because my code collects messages does not mean the data is automatically correct.

## 6. Why I Used Prefect, DuckDB, and dbt

I used these three tools because they each have a different job in my pipeline.

### Prefect

Prefect runs my tasks in the correct order and lets me see what is happening through its dashboard and logs.

I found the dashboard especially helpful because I could see which tasks were completed and where something went wrong.

Without Prefect, I would have to manage the order of the tasks and track failures myself.

### DuckDB

DuckDB stores the fragments after they are received from SQS.

I used it because I needed somewhere to save the messages before deleting them, and dbt could then work with the stored data.

Without DuckDB, I would not have the same database where I could store and check the fragments.

### dbt

dbt cleans the fragments, removes duplicates, checks for missing words, and reconstructs the phrase.

I think dbt was especially useful because it gave me a way to check the data before submitting an answer.

Without dbt, I would have to write the cleaning and validation logic somewhere else, and I would lose the separate testing step that my pipeline depends on.

## 7. Full-Delay Pipeline Run

After getting the pipeline to work in fast mode, I wanted to make sure it would also work with the original message delays.

I changed my `.env` settings to:

```env
DP2_FAST=0
DP2_CHAOS=0
```

With these settings, messages can be delayed anywhere from 30 to 900 seconds.

My full-delay run took **14 minutes and 22 seconds** to finish.

All six Prefect tasks completed successfully. The dbt build passed, and the submission returned HTTP status 200.

### Full-Delay Run Screenshot

![Full-delay Prefect flow](docs/full_delay_flow.png)

This run was important because it showed that my pipeline could wait for the messages without timing out or trying to reconstruct the phrase too early.

## 8. Chaos Mode Testing

Once the regular pipeline was working, I tested it using chaos mode.

I changed my settings to:

```env
DP2_FAST=1
DP2_CHAOS=1
```

In chaos mode, the Scatter API generates 24 messages instead of the usual 21:

- 21 valid fragments.
- 2 duplicate fragments.
- 1 malformed message.

### What Went Wrong at First

My original collection task was designed around receiving 21 messages.

This worked for the regular pipeline, but chaos mode made things more complicated because there were extra messages.

If I stopped collecting after receiving 21 messages, I could leave some messages in the queue. I also needed to make sure that duplicates did not get counted as new words.

### How I Fixed It

I updated my collection task so that it counts the number of distinct order numbers stored in DuckDB instead of just counting how many messages were received.

I also added a check for malformed messages.

If a message is missing an order number or word, my pipeline logs a warning and deletes the message without treating it as a valid fragment.

For valid messages, the pipeline saves the fragment in DuckDB before deleting it from SQS.

The collection task continues until:

1. There are 21 distinct order numbers stored in DuckDB.
2. The number of visible messages is zero.
3. The number of in-flight messages is zero.
4. The number of delayed messages is zero.

My dbt staging model also removes duplicate order numbers before reconstructing the phrase.

### Chaos Mode Results

I ran the pipeline twice in a row with chaos mode enabled, and both runs were successful.

In both runs:

- All six Prefect tasks completed.
- The pipeline collected 21 distinct fragments.
- Duplicate messages were handled.
- The malformed message was logged and deleted.
- The SQS queue was emptied.
- All six dbt build steps passed.
- The correct phrase was submitted with HTTP status 200.

When I checked the submissions, I received:

```text
PASS  xdd9uz / prefect  receipt: 43D5269990
PASS  xdd9uz / prefect  receipt: 43D5269990
2/2 submission(s) correct. Checked submissions were removed.
```

I was glad that both runs passed because it showed that the changes I made were working even when the queue contained extra messages.

## 9. Final Phrase and Verified Submission

After collecting and testing the fragments, my pipeline reconstructed the following phrase:

> Good tests do not prove your data is correct, but they keep the most embarrassing mistakes far away from your users.

The `submit_solution` task sent the phrase to the `dp2-submit` queue with these attributes:

- `uvaid`: `xdd9uz`
- `phrase`: The completed phrase.
- `platform`: `prefect`

The submission returned HTTP status 200.

I checked my submission using:

```bash
python kit/check_submission.py
```

### Verified Receipt Code

**43D5269990**

The submission checker confirmed that my answer was correct.

## 10. Conclusion

This project helped me understand how different tools work together in a data pipeline. At first, I was mainly focused on getting the messages into DuckDB and putting the words in the correct order. As I continued working, I realized that I also needed to think about what would happen if messages were delayed, duplicated, or missing information.

I learned why it is important to save messages before deleting them, use timeouts, and test the data before submitting it.

The most challenging part for me was making sure the collection task could handle chaos mode without leaving messages behind. After making those changes, I was able to run the pipeline successfully twice and verify both submissions.

## Project Files

- `prefect-flow.py` — My main Prefect pipeline.
- `dbt/` — My dbt models and tests.
- `kit/` — Files provided for the SQS emulator and submission checker.
- `docs/` — My DAG diagrams and screenshots.
- `SETUP.md` — Instructions for setting up the project.
- `CHECKPOINTS.md` — Project checkpoints.
- `RUBRIC.md` — Grading rubric and self-assessment.
- `PROGRESS.md` — Project progress notes.
