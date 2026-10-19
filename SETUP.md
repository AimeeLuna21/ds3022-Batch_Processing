
# Setup: running Project 2 on your laptop

You will have **four terminals** open while you work. Give each one a name so you never type in the wrong one:

| Terminal | Runs | Open it | Leave it running? |
|---|---|---|---|
| **T1 · Emulator** | `moto_server -p 9324` (your SQS queue) | first | Yes, while you work |
| **T2 · Scatter API** | `python kit/scatter_api.py` | second | Yes, while you work |
| **T3 · Work** | your flow, dbt, checks, git | any time | It is your working terminal |
| **T4 · Prefect** | `prefect server start` (the dashboard) | before your first flow run | Yes, while you work |

In every terminal, go to the project folder and **activate the virtual environment first** (your prompt starts with `(.venv)`). T1, T2, and T4 are servers: once started, they keep running and do not give you the prompt back. That is correct; do your typing in T3.

Once these services are running, the entire six-task pipeline can be executed with one command: `python prefect-flow.py`.

## 1. Python environment (once, in T3)

Use **Python 3.12 or 3.13**. I used Python 3.12 for this project.

First, clone the repository:

```bash
git clone https://github.com/AimeeLuna21/ds3022-Batch_Processing.git
cd ds3022-Batch_Processing
```

macOS / Linux / GitHub Codespaces:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Windows (PowerShell):

```powershell
py -3.12 -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
```

Open `.env` and set `UVA_ID` to your UVA computing ID. Make sure the file contains these settings:

```env
UVA_ID=xdd9uz
AWS_ENDPOINT_URL_SQS=http://127.0.0.1:9324
AWS_DEFAULT_REGION=us-east-1
AWS_ACCESS_KEY_ID=testing
AWS_SECRET_ACCESS_KEY=testing
SCATTER_PORT=8000
DP2_FAST=1
DP2_CHAOS=0
DP2_DUCKDB=dp2.duckdb
```

The AWS credentials are dummy values because this project uses a local emulator. The `.env` file should not be committed to GitHub.

To activate the virtual environment in a new terminal later, use `source .venv/bin/activate` (macOS/Linux) or `.venv\Scripts\Activate.ps1` (Windows).

## 2. Start the SQS emulator (T1)

```bash
moto_server -p 9324
```

Leave it running. Moto keeps queues **in memory**: if you stop it, every queue and message disappears (unlike real SQS). Start it again and run the flow; the POST request to the Scatter API creates the queues again.

Port 9324 is used on purpose. Moto's default port (5000) may be occupied by AirPlay on macOS.

**Alternative (Docker):** Instead of Moto, you can run ElasticMQ on the same port:

```bash
docker compose -f kit/docker-compose.elasticmq.yml up -d
```

Use one emulator or the other, not both. I used Moto for my project and did not separately test ElasticMQ.

## 3. Start the Scatter API (T2)

```bash
python kit/scatter_api.py
```

It prints the port, emulator address, and delay mode.

`DP2_FAST=1` in `.env` gives 5–60 second delays for development. For the full-delay run, set `DP2_FAST=0` (30–900 second delays, so allow about 15 minutes).

The Scatter API reads `.env` when it starts. After changing `DP2_FAST` or `DP2_CHAOS`, press **Ctrl+C** in T2 and start it again.

## 4. Check everything (T3)

```bash
python kit/smoke_test.py
```

You are ready when the last line says:

```text
Emulator OK
```

Keep T1 and T2 running afterward. The flow needs both every time it runs.

## 5. Start the Prefect server (T4)

First, configure Prefect to connect to the local server:

```bash
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
```

Then start the server:

```bash
prefect server start
```

Open the dashboard at http://127.0.0.1:4200.

**Important:** Make sure `PREFECT_API_URL` is configured before running `python prefect-flow.py` in T3. The Moto emulator, Scatter API, and Prefect server should all be running before starting the pipeline.

The dashboard shows the flow runs and their logs. I also used it to capture screenshots for my README.

## 6. Day-to-day commands (T3)

Once all three services are running, use:

```bash
python prefect-flow.py
```

This single command executes the complete pipeline, including the Scatter API request, queue monitoring, message collection, dbt build, phrase reconstruction, and submission.

Other useful commands:

```bash
dbt build --project-dir dbt --profiles-dir dbt
python kit/check_submission.py
```

The dbt command is only needed when testing dbt separately. During a normal pipeline run, Prefect executes dbt automatically.

The submission checker verifies pending submissions and removes the checked messages, so it should be used when you intend to consume them.

To inspect the queue from Python:

```python
import boto3
from dotenv import load_dotenv

load_dotenv()
sqs = boto3.client("sqs")
print(sqs.list_queues())
```

### Testing modes

For fast mode:

```env
DP2_FAST=1
DP2_CHAOS=0
```

For the full-delay run:

```env
DP2_FAST=0
DP2_CHAOS=0
```

For chaos mode:

```env
DP2_FAST=1
DP2_CHAOS=1
```

After changing modes, restart the Scatter API in T2 before running the pipeline again.

## 7. When you stop working

Press **Ctrl+C** in T4, then T2, then T1, and close them. In T3, type `deactivate`.

Next time, start the services again in this order: T1, T2, T4, then run the pipeline in T3.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Could not connect to the endpoint URL: http://127.0.0.1:9324` | T1 is not running `moto_server -p 9324`, or the emulator address is incorrect. |
| `ConnectionError` / `Connection refused` on port 8000 | T2 is not running `python kit/scatter_api.py`. |
| `ReadTimeout ... port=8000 ... read timeout=30` (Windows) | Your `.env` or code may still say `localhost`. Use `127.0.0.1` in `AWS_ENDPOINT_URL_SQS` and `SCATTER_URL`, restart T2, and run again. |
| `Address already in use` / port 9324, 8000, or 4200 busy | That server may already be running in another terminal. Use that terminal or stop the existing server. |
| `command not found: moto_server` or `No module named ...` | The virtual environment may not be active, or dependencies may not be installed. |
| `NoCredentialsError` or `NoRegionError` | Check the AWS credentials and region in `.env`. |
| `KeyError: 'UVA_ID'` | `.env` has no `UVA_ID` line, or you ran the command outside the project folder. |
| Run not in the dashboard | T4 is not running, or `PREFECT_API_URL` is not configured. |
| Messages have no `MessageAttributes` | Pass `MessageAttributeNames=["All"]` to `receive_message`. |
| `KeyError: 'Messages'` | Nothing was visible yet. Use `resp.get("Messages", [])`. |
| `Can't open a connection to same database file with a different configuration` | DuckDB may have conflicting connection settings. Avoid opening the same database read-only while another connection is writing. |
| dbt creates `dp2.duckdb` in the wrong folder | Run dbt from the repository root with `--project-dir dbt --profiles-dir dbt`. |
| `pip install` fails on dbt | Check `python --version` and use Python 3.12 or 3.13. |
| PowerShell will not run `Activate.ps1` | Run `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, then try again. |
| Queues vanished after restarting the emulator | Expected: Moto starts empty. Run the flow again to recreate the queues. |
 