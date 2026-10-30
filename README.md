# Retail Analytics Platform on AWS

## What this is

I built a retail analytics platform on AWS. It generates synthetic Customers,
Products, Orders, Payments, Inventory movements, and Clickstream activity.

Those domains do not need the same data model. Customer and Product attributes
need history. Orders and Payments need a resolved current state. Inventory and
Clickstream need the underlying event history.

Each run lands data in S3, models it as Iceberg Silver and Gold tables, then
publishes selected Gold marts to Redshift for queries and charts. Batch files
and Kinesis Clickstream events arrive on different paths, so the workflow waits
for the right deliveries before the downstream tables are built.

## What the system looks like

<img src="assets/architecture/retail-analytics-platform-architecture.webp" alt="Architecture diagram showing retail batch and Clickstream ingestion through S3 Bronze, Glue Silver, EMR Gold, and Redshift, with Step Functions orchestration and monitoring" width="100%">

<p align="center"><em>System architecture</em></p>

Batch and Clickstream data meet in S3 Bronze. I kept Silver in Glue, Gold in
EMR Serverless, and Redshift focused on serving rather than transformation.

## Where the code lives

```text
lambda/              Retail and Clickstream generators plus S3 helpers
glue/                Bronze to Silver Glue Spark jobs
emr_serverless/      Silver to Gold EMR Serverless Spark jobs
step_function/       Step Functions state machine and redacted input example
redshift/            Redshift serving layer SQL
assets/architecture/  Architecture diagrams created for the project
assets/screenshots/   Redacted AWS Console evidence, grouped by platform layer
```

I created the AWS resources manually in the Console, so there is no Terraform,
CDK, or CloudFormation in this repository.

## How data is modelled

| Layer | What lives there | Main technologies |
|---|---|---|
| Bronze | Immutable source files and events that can be replayed | Lambda, Kinesis, Firehose, S3 |
| Silver | Validated and deduplicated tables with CDC and SCD2 modelling | Glue Spark, Iceberg, Glue Data Catalog |
| Gold | Business marts and session metrics | EMR Serverless Spark, Iceberg |
| Serving | Native reporting tables, views, SQL, and charts | Redshift Serverless |

<br>

<img src="assets/architecture/retail-data-lineage.webp" alt="Data lineage from Bronze domains through Silver modelling patterns to Gold marts and Redshift serving tables" width="100%">

<p align="center"><em>Data lineage, modelling choices, and the session to funnel dependency</em></p>

### Bronze datasets I can replay

Bronze is immutable source data in S3 for Customers, Products, Orders, Payments,
Inventory, and Clickstream. That gives me a stable replay point. If I change a
Silver transformation, I can process the landed files again instead of
regenerating the source data.

The batch domains land in files partitioned by date with shared run metadata.
Clickstream lands as NDJSON from Firehose and is also partitioned by
`run_id`, which lets the workflow wait for the exact run's files.

| Bronze dataset | What lands there | Why keep it in Bronze? |
|---|---|---|
| Customers | Source customer records and ingestion metadata | Lets Silver build customer history again if its SCD2 logic changes |
| Products | Source product records and ingestion metadata | Preserves catalog inputs before product history is modelled |
| Orders | Raw order CDC events | Keeps the insert, update, and delete events needed to rebuild current order state |
| Payments | Raw payment CDC events | Keeps the payment lifecycle events needed to rebuild current payment state |
| Inventory | Raw stock movement events | Preserves every movement for replay and inventory aggregation |
| Clickstream | NDJSON behavioural events from Firehose | Keeps the event stream available for session and funnel processing |

### Silver tables each domain gets the shape it needs

Silver is where raw inputs become validated Iceberg tables. I did not use one
pattern everywhere. Customers and Products use SCD Type 2 because their
attribute history matters. Orders and Payments use CDC and MERGE because the
latest state is what downstream metrics need. Inventory and Clickstream keep
append event records because the event history itself is useful.

| Silver table | Grain / pattern | Why this shape? |
|---|---|---|
| `silver_customers` | One customer version; SCD Type 2 | Customer attributes change over time, so the history and current version both matter |
| `silver_products` | One product version; SCD Type 2 | Product attributes and pricing can change, so Gold can use the current version while history remains available |
| `silver_orders` | One current row per order; CDC/MERGE | Downstream sales and customer metrics need one resolved latest state per order |
| `silver_payments` | One current row per payment; CDC/MERGE | Payment reporting needs the resolved latest status rather than every intermediate event |
| `silver_inventory` | One inventory movement event stored as an append event | Inventory Health depends on the movement history, not only the latest event |
| `silver_clickstream` | One deduplicated event ID stored as an append event | Session analytics and funnels depend on individual behavioural events |

### The Gold marts

| Mart | What it is for |
|---|---|
| Product Performance | Product revenue, units, margin, engagement, and inventory movement |
| Clickstream Session Analytics | One row per session with event counts, duration, device context, and funnel flags |
| Conversion Funnel | Device level product view, cart, checkout, and purchase conversion |
| Customer Metrics | Purchase behaviour, lifecycle status, payment outcomes, and preferences |
| Daily Sales | Daily category level order, revenue, and payment metrics |
| Inventory Health | Product and warehouse inventory positions, movements, and stock status |
| Customer Lifetime Value | Observed customer value, purchase frequency, recency, and payment metrics |

<img src="assets/screenshots/lakehouse/glue-catalog-silver-tables.webp" alt="Six Silver Apache Iceberg tables in AWS Glue Data Catalog" width="100%">

<p align="center"><em>Silver Iceberg tables in the Glue Data Catalog</em></p>

<img src="assets/screenshots/lakehouse/glue-catalog-gold-tables.webp" alt="Seven Gold Apache Iceberg tables in AWS Glue Data Catalog" width="100%">

<p align="center"><em>Gold Iceberg marts in the Glue Data Catalog</em></p>

## Under the hood

These screenshots show the AWS setup used by the project.

<details open>
<summary><strong>Ingestion setup with Lambda, Kinesis, and Firehose</strong></summary>

<br>

<img src="assets/screenshots/ingestion/lambda-function-configuration.webp" alt="Lambda function configuration" width="100%">

<p align="center"><em>Lambda data generator</em></p>

<img src="assets/screenshots/ingestion/lambda-layer-configuration.webp" alt="Lambda layer configuration" width="100%">

<p align="center"><em>Lambda dependency layer</em></p>

<img src="assets/screenshots/ingestion/kinesis-data-stream-configuration.webp" alt="Kinesis Data Streams configuration" width="100%">

<p align="center"><em>Kinesis Data Stream for Clickstream events</em></p>

<img src="assets/screenshots/ingestion/firehose-dynamic-partitioning.webp" alt="Firehose run ID dynamic partitioning configuration" width="100%">

<p align="center"><em>Firehose dynamic partitioning by <code>run_id</code></em></p>

<img src="assets/screenshots/ingestion/firehose-destination-settings.webp" alt="Firehose S3 destination settings" width="100%">

<p align="center"><em>Firehose S3 landing destination</em></p>

</details>

<details open>
<summary><strong>Lakehouse storage and catalog</strong></summary>

<br>

<img src="assets/screenshots/lakehouse/s3-platform-bucket-layout.webp" alt="S3 bucket layout for the retail analytics platform" width="100%">

<p align="center"><em>S3 platform layout</em></p>

<img src="assets/screenshots/lakehouse/s3-bronze-layer.webp" alt="S3 Bronze layer" width="100%">

<p align="center"><em>Immutable Bronze storage</em></p>

<img src="assets/screenshots/lakehouse/s3-bronze-data.webp" alt="Bronze data files in S3" width="100%">

<p align="center"><em>Bronze data files</em></p>

<img src="assets/screenshots/lakehouse/glue-catalog-databases.webp" alt="AWS Glue Data Catalog databases" width="100%">

<p align="center"><em>Glue Catalog databases</em></p>

<img src="assets/screenshots/lakehouse/s3-silver-layer.webp" alt="S3 Silver layer" width="100%">

<p align="center"><em>Silver Iceberg storage</em></p>

<img src="assets/screenshots/lakehouse/s3-silver-data.webp" alt="Silver Iceberg table data in S3" width="100%">

<p align="center"><em>Silver Iceberg table data</em></p>

<img src="assets/screenshots/lakehouse/s3-silver-iceberg-metadata.webp" alt="Silver Iceberg table metadata in S3" width="100%">

<p align="center"><em>Silver Iceberg metadata</em></p>

<img src="assets/screenshots/lakehouse/s3-gold-layer.webp" alt="S3 Gold layer" width="100%">

<p align="center"><em>Gold Iceberg storage</em></p>

<img src="assets/screenshots/lakehouse/s3-gold-iceberg-table-data.webp" alt="Gold Iceberg table data in S3" width="100%">

<p align="center"><em>Gold mart data files</em></p>

</details>

<details open>
<summary><strong>Processing setup with Glue and EMR Serverless</strong></summary>

<br>

<img src="assets/screenshots/processing/glue-etl-jobs_converted.webp" alt="AWS Glue ETL job definitions" width="100%">

<p align="center"><em>Glue ETL job definitions</em></p>

<img src="assets/screenshots/processing/glue-etl-job-script_converted.webp" alt="AWS Glue ETL job script configuration" width="100%">

<p align="center"><em>A Glue job pointing at its transformation script</em></p>

<img src="assets/screenshots/processing/glue-job-runs_converted.webp" alt="Successful AWS Glue job runs" width="100%">

<p align="center"><em>Glue job runs for the Silver layer</em></p>

<img src="assets/screenshots/processing/emr-serverless-application_converted.webp" alt="EMR Serverless application configuration" width="100%">

<p align="center"><em>EMR Serverless Spark application</em></p>

<img src="assets/screenshots/processing/emr-serverless-job-runs_converted.webp" alt="Successful EMR Serverless Gold job runs" width="100%">

<p align="center"><em>EMR Serverless job runs for Gold marts</em></p>

</details>

<details open>
<summary><strong>Orchestration and operational setup</strong></summary>

<br>

<img src="assets/screenshots/orchestration/step-functions-designer_converted.webp" alt="Step Functions state machine designer" width="100%">

<p align="center"><em>Step Functions state machine design</em></p>

<img src="assets/screenshots/orchestration/step-functions-job-runs_converted.webp" alt="Step Functions execution with downstream jobs" width="100%">

<p align="center"><em>State machine execution coordinating downstream jobs</em></p>

<img src="assets/screenshots/orchestration/step-functions-historical-simulation_converted.webp" alt="Step Functions historical simulation run" width="100%">

<p align="center"><em>Historical simulation run</em></p>

<img src="assets/screenshots/orchestration/eventbridge-schedule_converted.webp" alt="EventBridge Scheduler configuration" width="100%">

<p align="center"><em>EventBridge Scheduler starting the state machine</em></p>

<img src="assets/screenshots/operations/sns-alert-topic.webp" alt="SNS alert topic" width="100%">

<p align="center"><em>SNS pipeline notification topic</em></p>

<img src="assets/screenshots/operations/step-functions-success-alert.webp" alt="Step Functions success alert" width="100%">

<p align="center"><em>Step Functions success notification</em></p>

<img src="assets/screenshots/operations/step-functions-failure-alert.webp" alt="Step Functions failure alert" width="100%">

<p align="center"><em>Step Functions failure notification</em></p>

<img src="assets/screenshots/operations/sqs-scheduler-dlq.webp" alt="SQS Scheduler dead-letter queue configuration" width="100%">

<p align="center"><em>Scheduler dead letter queue</em></p>

</details>

## What one run does

1. **Lambda starts the run**

   Lambda creates one `run_id` and one processing date. It writes Customers,
   Products, Orders, Payments, and Inventory to Bronze. It sends Clickstream
   events to Kinesis.

2. **Batch data is ready for Silver**

   Step Functions receives the exact S3 paths Lambda returned for the five
   batch domains. It can start those five Glue jobs immediately.

3. **Clickstream takes the streaming route**

   Kinesis sends the events to Firehose. Firehose reads the `run_id` from each
   event and writes its NDJSON output below that run's Clickstream S3 prefix.
   Step Functions checks the prefix until it finds a delivered object.

4. **Glue builds Silver**

   Clickstream Silver starts only after Firehose delivery is visible. By then,
   the five batch Silver jobs may already be running or complete. Each domain
   applies its own model such as SCD Type 2 history, CDC resolution, or append
   event storage.

5. **EMR builds Gold**

   The Gold marts begin after Silver is done. Conversion Funnel waits for
   Clickstream Session Analytics because it aggregates the session metrics it
   produces.

6. **Redshift refreshes the serving layer**

   The final optional step runs the Redshift refresh procedure. It copies the
   selected Gold marts into native serving tables and views for the SQL queries
   and charts.

<img src="assets/architecture/kinesis-aware-run-lifecycle.webp" alt="Kinesis aware pipeline run lifecycle with parallel batch and Clickstream processing" width="100%">

<p align="center"><em>Kinesis aware pipeline run lifecycle</em></p>

<img src="assets/screenshots/orchestration/step-functions-kinesis-success_converted.webp" alt="Successful Kinesis aware Step Functions execution" width="100%">

<p align="center"><em>A complete Kinesis aware Step Functions run</em></p>

### What I checked

**Ingestion**

Lambda produced the five batch files with shared run metadata. It also
published Clickstream events to Kinesis. Firehose extracted `run_id` and
landed NDJSON in the matching S3 prefix.

**Transformations**

All six Glue Silver jobs ran from Bronze inputs. All seven EMR Serverless Gold
jobs ran from Silver. That covered validation, deduplication, CDC and MERGE,
SCD Type 2 history, append event handling, and Iceberg writes.

**Workflow and serving**

The Kinesis aware Step Functions route completed through Firehose delivery
detection, Silver, Gold, Conversion Funnel, and a Redshift refresh. Bootstrap
SQL, the serving refresh procedure, the native tables and views, and the demo
queries all ran.

**Scheduling and multiple dates**

EventBridge Scheduler started the workflow. SNS notifications and the
Scheduler SQS dead letter queue were configured and exercised. I also ran
simulated dates from `2026-10-01` through `2026-10-04` and refreshed Redshift
on the final date.

## What the system produces

Gold Iceberg tables remain the analytical source of truth. Redshift copies the
chosen marts into native serving tables and views, which gave me a place to run
analyst SQL and make charts.

<img src="assets/screenshots/serving/redshift-daily-gross-revenue_converted.webp" alt="Daily gross revenue chart in Redshift Query Editor v2" width="100%">

<p align="center"><em>Daily gross revenue</em></p>

<img src="assets/screenshots/serving/redshift-daily-revenue-by-category_converted.webp" alt="Daily revenue by product category chart in Redshift Query Editor v2" width="100%">

<p align="center"><em>Daily revenue by product category</em></p>

<img src="assets/screenshots/serving/redshift-inventory-risk_converted.webp" alt="Inventory risk chart in Redshift Query Editor v2" width="100%">

<p align="center"><em>Inventory positions below the stock threshold</em></p>

<img src="assets/screenshots/serving/redshift-payment-performance_converted.webp" alt="Payment performance chart in Redshift Query Editor v2" width="100%">

<p align="center"><em>Payment performance</em></p>

<img src="assets/screenshots/serving/redshift-conversion-funnel_converted.webp" alt="Conversion funnel chart in Redshift Query Editor v2" width="100%">

<p align="center"><em>Conversion funnel by session stage</em></p>

<img src="assets/screenshots/serving/redshift-customer-lifetime-value_converted.webp" alt="Customer lifetime value chart in Redshift Query Editor v2" width="100%">

<p align="center"><em>Customer lifetime value</em></p>

<img src="assets/screenshots/serving/redshift-customer-segmentation_converted.webp" alt="Customer segmentation chart in Redshift Query Editor v2" width="100%">

<p align="center"><em>Customer segmentation</em></p>

## Reproducing it

This is a manual deployment. I created the AWS resources in
the Console, so anyone reproducing it needs their own AWS account, bucket,
IAM roles, and values for the account.

At a minimum, set up Lambda, S3, Kinesis Data Streams, Firehose, Glue, EMR
Serverless, Step Functions, Redshift Serverless, EventBridge Scheduler,
CloudWatch, SNS, and SQS. Upload the Lambda, Glue, and EMR scripts to locations
your roles can read, configure the Glue and EMR Iceberg catalog settings, then
create the state machine with ARNs and resource IDs from the account.

The redacted execution input shape is available in
[`step_function/state_machines/input_template_example.json`](step_function/state_machines/input_template_example.json).
Use it as a reference, then replace every placeholder with values from your own
account. The diagrams and source files show which service owns each part.

## License

Released under the [MIT License](LICENSE).

## A few choices I made

I made Step Functions wait for the matching Firehose delivery instead of
assuming Clickstream arrives with the batch files. I stored Customers and
Products as SCD Type 2 tables. Orders and Payments resolve to current state.
Inventory and Clickstream keep event history.

I kept Gold in the lakehouse and gave Redshift native reporting tables and
views. That keeps transformation logic in the lakehouse and gives query work a
separate place to live.
