Heroku AppLink - Scaling Batch Jobs with Heroku - Node.js
=========================================================

This sample seamlessly delegates the processing of large amounts of data with significant compute requirements to Heroku Worker processes. It also demonstrates the use of the Unit of Work aspect of the SDK for easier utilization of the Salesforce Composite APIs.

# Architecture Overview

The scenario used in this sample illustrates a basis for processing large volumes of Salesforce data using elastically scalable Heroku worker processes that execute complex compute calculations. In this case **Opportunity** data is read and calculated pricing data is stored in an associated **Quote**. Calculating quote information from opportunities can become quite intensive, especially when large multinational businesses have complex rules that impact pricing related to region, products, and discount thresholds. It's also possible that such code already exists, and there is a desire to reuse it within a Salesforce context. 

<img src="images/arch.jpg" width="80%">

This sample includes two process types `web` and `worker`, both can be scaled vertically and horizontally to speed up processing. The `web` process will receive API calls from Salesforce and `worker` will execute the jobs asynchronously. A [Heroku Key Value Store](https://elements.heroku.com/addons/heroku-redis) (Heroku Redis) is used to create means to communicate between the two processes.

> [!NOTE]
> This sample could be considered an alternative to using Batch Apex if your data volumes and/or compute complexity requires it. In addition Heroku worker processes scale elastically and can thus avoid queue wait times impacting processing time that can occur with Batch Apex. For further information see **Technical Information** below.

# Requirements
- Heroku login
- Heroku CLI installed
- Heroku AppLink plugin installed
- Salesforce CLI installed
- Login information for one or more Scratch, Development or Sandbox orgs

# Local Development and Testing

As with other samples (see below) this section focuses on how to develop and test locally before deploying to Heroku and testing from within a Salesforce org. Using the `heroku local` command (part of the Heroku CLI) we can easily launch locally both the `web` and `worker` processes defined in the `Procfile` from one command. Run the following commands to run the sample locally against a remotely provisioned [Heroku Key Value Store](https://devcenter.heroku.com/articles/heroku-redis).

> [!IMPORTANT]
> If have deployed the application, as described below and want to return to local development, you may want to destroy it to avoid race conditions since both will share the same job queue, use `heroku destroy`. In real situation you would have a different queue store for developer vs production.

Start with the following commands to create an empty application and provision within Heroku a key value store this sample uses to manage the job queue:

```sh
# Create app and add Redis
heroku create
heroku addons:create heroku-redis:mini --wait

# Copy Heroku Redis URL to local .env file
heroku config --shell > .env

# Install dependencies
pnpm install

# Run locally (starts both web and worker as defined in Procfile)
heroku local -f Procfile.local web=1,worker=1
```

### Generating test data

Open a new terminal window and enter the following command to start a job that generates sample data:

```sh
# Use the invoke script (replace my-org with your org alias)
# Target: POST /api/data/create (Default: 10 Opportunities)
./bin/invoke.sh my-org 'http://localhost:5000/api/data/create' '{}'

# To specify a different number (e.g., 100), use the query parameter:
./bin/invoke.sh my-org 'http://localhost:5000/api/data/create?numberOfOpportunities=100' '{}'
```

This will respond with a job Id, as shown in the example below:

```json
Response from server:
{"jobId":"b7bfb6bd-8db8-4e4f-b0ad-c98966e91dde"}
```

Review the log output from the `heroku local` process and you will see output similar to the following (timestamps and specific IDs will vary):

```
web.1    | Job published to Redis channel jobsChannel...
worker.1 | Worker received job with ID: b63e2cbd-cb6a-4be9-b2e1-0b1ab928938b for data operation: create
worker.1 | Starting data creation via Bulk API v2 for Job ID: b63e2cbd-cb6a-4be9-b2e1-0b1ab928938b, Count: 10
worker.1 | Preparing Bulk API v2 Opportunity creation job for Job ID: b63e2cbd-cb6a-4be9-b2e1-0b1ab928938b
worker.1 | Submitted Bulk API v2 Opportunity creation job with ID: 750am00000Q3m1BAAR...
worker.1 | Polling Bulk API v2 job status for Job ID: 750am00000Q3m1BAAR...
worker.1 | Bulk API v2 Job 750am00000Q3m1BAAR status: UploadComplete
worker.1 | Bulk API v2 Job 750am00000Q3m1BAAR status: InProgress
worker.1 | Bulk API v2 Job 750am00000Q3m1BAAR status: JobComplete
worker.1 | Bulk API v2 Job 750am00000Q3m1BAAR processing complete.
worker.1 | Opportunity creation job 750am00000Q3m1BAAR completed. State: JobComplete, Processed: 10, Failed: 0
worker.1 | Extracted 10 successful Opportunity IDs for Job ID: b63e2cbd-cb6a-4be9-b2e1-0b1ab928938b
worker.1 | Preparing Bulk API v2 OLI creation job for 10 Opportunities...
worker.1 | Submitted Bulk API v2 OLI creation job with ID: 750am00000Q3zmNAAR...
worker.1 | Polling Bulk API v2 job status for Job ID: 750am00000Q3zmNAAR...
worker.1 | Bulk API v2 Job 750am00000Q3zmNAAR status: InProgress
worker.1 | Bulk API v2 Job 750am00000Q3zmNAAR status: JobComplete
worker.1 | Bulk API v2 Job 750am00000Q3zmNAAR processing complete.
worker.1 | OLI creation job 750am00000Q3zmNAAR completed. State: JobComplete, Processed: 20, Failed: 0
worker.1 | Job processing completed for Job ID: b63e2cbd-cb6a-4be9-b2e1-0b1ab928938b
```

Finally navigate to the **Opportunities** tab in your Salesforce org and you should see something like the following

<img src="images/opps.jpg" width="60%">

### Running the generate quotes job locally

Run the following command to execute a batch job to generate **Quote** records from the **Opportunity** records created above.

```sh
# Target: POST /api/executebatch
./bin/invoke.sh my-org http://localhost:5000/api/executebatch '{"soqlWhereClause": "Name LIKE '\''Sample Opportunity%'\''"}'
```

Observe the log output from `heroku local` and you will see output similar to the following:

```
web.1    | Job published to Redis channel jobsChannel...
worker.1 | Worker received job with ID: 778412d8-f56f-4a11-ad62-09174339e5f9 for SOQL WHERE clause: Name LIKE 'Sample Opportunity%'
worker.1 | Processing 10 Opportunities
worker.1 | Submitting UnitOfWork to create 10 Quotes and 20 Line Items
worker.1 | Job processing completed for Job ID: 778412d8-f56f-4a11-ad62-09174339e5f9. Results: 10 succeeded, 0 failed.
```

Navigate to the **Quotes** tab in your org to review the generates records:

<img src="images/quotes.jpg" width="60%">

Next we will deploy the application and pushing it into a Salesforce org to allow jobs to be started from Apex, Flow or Agentforce.

# Deploying and Testing

> [!IMPORTANT]
> Check you are not still running the application locally. If you want to start over at any time use `heroku destroy` to delete your app.

Steps below leverage the `sf` CLI as well so please ensure you have authenticated your org already - if not you can use this command:

```
sf org login web --alias my-org
```

First, if you have not done so above, create the application and provision a key value store to manage the job queue.

```sh
heroku create
heroku addons:create heroku-redis:mini --wait
```

Next config the Heroku AppLink add-on and publish the application into your Salesforce org as follows:

```
heroku addons:create heroku-applink --wait
heroku buildpacks:add --index=1 heroku/heroku-applink-service-mesh
heroku buildpacks:add heroku/java
heroku config:set HEROKU_APP_ID="$(heroku apps:info --json | jq -r '.app.id')"
heroku salesforce:connect my-org
heroku salesforce:publish api-docs.yaml --client-name GenerateQuoteJob --connection-name my-org --authorization-connected-app-name GenerateQuoteJobConnectedApp --authorization-permission-set-name GenerateQuoteJobPermissions
```

Finally deploy the application and scale both the `web` and `worker` processes to run on a single dyno each. If your worker does not start, login to the Heroku dashboard and check the worker is enabled.

```
git push heroku main
heroku ps:scale web=1,worker=1
```

Now grant permissions to users to invoke your code using the following `sf` commands:

```
sf org assign permset --name GenerateQuoteJob -o my-org
sf org assign permset --name GenerateQuoteJobPermissions -o my-org
```

Once published you can see the `executeBatch` operation that takes a [SOQL WHERE clause](https://developer.salesforce.com/docs/atlas.en-us.soql_sosl.meta/soql_sosl/sforce_api_calls_soql_select_conditionexpression.htm) to select the **Opportunity** object records to process. Also note that the `datacreate` and `datadelete` operations are also exposed since they declared in the `api-docs.yaml` generated from the Java annotations within `PriceEngineService.java`. 

<img src="images/imported.jpg" width="60%">

As noted in the [Extending Apex, Flow and Agentforce](https://github.com/heroku-examples/heroku-integration-pattern-org-action-java?tab=readme-ov-file#heroku-integration---extending-apex-flow-and-agentforce---java) sample you can now invoke these operations from Apex, Flow or Agentforce. Here is some basic Apex code to start the job to create the sample data (if you have not done so earlier):

```
echo \
"HerokuAppLink.GenerateQuoteJob service = new HerokuAppLink.GenerateQuoteJob();" \
"HerokuAppLink.GenerateQuoteJob.datacreate_Request request = new HerokuAppLink.GenerateQuoteJob.datacreate_Request();" \
"System.debug('Quote Id: ' + service.datacreate(request).Code200.jobId);" \
| sf apex run -o my-org
```

> [!NOTE]
> Run the `heroku logs --tail` command to monitor the logs to confirm the job completed.

Here is some basic Apex code you can run from the command line to start the generate Quotes job:

```
echo \
"HerokuAppLink.GenerateQuoteJob service = new HerokuAppLink.GenerateQuoteJob();" \
"HerokuAppLink.GenerateQuoteJob.executeBatch_Request request = new HerokuAppLink.GenerateQuoteJob.executeBatch_Request();" \
"HerokuAppLink.GenerateQuoteJob_BatchExecutionRequest body = new HerokuAppLink.GenerateQuoteJob_BatchExecutionRequest();" \
"body.soqlWhereClause = 'Name LIKE \\\\'Sample Opportunity%\\\\'';" \
"request.body = body;" \
"System.debug('Quote Id: ' + service.executeBatch(request).Code200.jobId);" \
| sf apex run -o my-org
```

> [!NOTE]
> Run the `heroku logs --tail` command to monitor the logs of the `web` and `worker` processes as you did when running locally.

Navigate to the **Quotes** tab in your org or one of the sample **Oppoortunties** to review the generates quotes. You can re-run this operation as many times as you like it will simply keep adding **Quotes** to the sample Opporunties created.

# Removing Sample Data

If you are running application locally, run the following command to execute a batch process to delete the sample **Opportunity** and **Quote** records.

```sh
# Target: POST /api/data/delete
./bin/invoke.sh my-org http://localhost:5000/api/data/delete '{}'
```

If you have deployed the application, run the following:

```
echo \
"HerokuAppLink.GenerateQuoteJob service = new HerokuAppLink.GenerateQuoteJob();" \
"System.debug('Quote Id: ' + service.datadelete().Code200.jobId);" \
| sf apex run -o my-org
```

Observe the log output from the `heroku local` or `heroku logs --tail` commands and you will see output similar to the following

```
web.1    | Job published to Redis channel jobsChannel...
worker.1 | Worker received job with ID: 55610381-55f8-4a05-8550-158f6410663b for data operation: delete
worker.1 | Starting data deletion via Bulk API v2 for Job ID: 55610381-55f8-4a05-8550-158f6410663b
worker.1 | Found 10 Opportunities to delete for Job ID: 55610381-55f8-4a05-8550-158f6410663b
worker.1 | Preparing Bulk API v2 Opportunity deletion job for Job ID: 55610381-55f8-4a05-8550-158f6410663b
worker.1 | Submitted Bulk API v2 Deletion job with ID: 750am00000Q3uV1AAJ...
worker.1 | Polling Bulk API v2 job status for Job ID: 750am00000Q3uV1AAJ...
worker.1 | Bulk API v2 Job 750am00000Q3uV1AAJ status: UploadComplete
worker.1 | Bulk API v2 Job 750am00000Q3uV1AAJ status: InProgress
worker.1 | Bulk API v2 Job 750am00000Q3uV1AAJ status: JobComplete
worker.1 | Bulk API v2 Job 750am00000Q3uV1AAJ processing complete.
55610381-55f8-4a05-8550-158f6410663b
```

# Technical Information
- The [Heroku Redis](https://elements.heroku.com/addons/heroku-redis) add-on is used to manage job queuing via a single Redis Stream named `jobsChannel`. While there are logically two types of jobs (quote generation and sample data management), both are sent to this same stream. The worker process reads from this stream and dispatches jobs to the appropriate service based on the job type included in the message payload. The `mini` tier of this [add-on](https://devcenter.heroku.com/articles/heroku-redis) is suitable for this sample. Redis connection details are managed via environment variables (typically set in `.env` locally or via Heroku Config Vars).
- Node.js and the `Procfile` define the `web` and `worker` process types. The `web` process runs the Fastify server (`server/index.js`) handling API requests and publishing jobs to the Redis stream, while the `worker` process (`server/worker.js`) listens to the stream and executes jobs using the appropriate service (`server/services/quote.js` or `server/services/data.js`).
- The quote generation logic in `server/services/quote.js` uses the AppLink SDK's Data API and Unit of Work pattern (`org.dataApi.newUnitOfWork`, `commitUnitOfWork`) to insert **Quote** and **QuoteLineItem** records together within a single transaction, ensuring atomicity.
- The `invoke.sh` script relies on the `x-client-context` header being correctly passed for authentication when running locally. The main `Procfile` is used for deployment, which incorporates the Heroku AppLink service mesh.
- The main worker process (`server/worker.js`) receives job messages via Redis, extracts and initializes the Salesforce context, and then delegates the core processing logic (using the context) to handlers defined in `server/services/quote.js` and `server/services/data.js`.
- The [Heroku Connect](https://elements.heroku.com/addons/herokuconnect) add-on can be used as an alternative to reading and/or writing to an org via [Heroku Postgres](https://elements.heroku.com/addons/heroku-postgresql). This is an option to consider if your use case does not fit within the [Salesforce API limitations](https://developer.salesforce.com/docs/atlas.en-us.salesforce_app_limits_cheatsheet.meta/salesforce_app_limits_cheatsheet). In this case note that there will be some lag between data changes and updates in the Salesforce org caused by the nature of the synchronization pattern used by Heroku Connect. If this is acceptable this option will further increase performance. Of course a hybrid of using the Salesforce API for certain data access needs and Heroku Connect for others is also possible.
- This sample uses [Salesforce API Query More](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_query_more_results.htm) pattern implicitly via the AppLink SDK's `org.dataApi.query` method when retrieving large datasets.
- To create sample data (`handleDataMessage` in `server/services/data.js`), the AppLink SDK's Bulk API v2 (`org.bulkApi`) is used for efficient handling of potentially large volumes.
- **An informal execution time comparison.** The pricing calculation logic is intentionally simple for the purposes of ensuring the technical aspects of using the Heroku AppLink in this context are made clear. As the compute requirements fit within Apex limits, it was possible to create an Apex version of the job logic (originally included in the Java version's `/src-org` folder). While not a formal benchmark, execution time over 5000 opportunities took ~24 seconds using the Heroku job approach vs ~150 seconds to run with Batch Apex, **an improvement of 144% in execution time**. During testing (of the Java version) it was observed that this was largely due in this case to the longer dequeue times with Batch Apex vs being near instant with a Heroku worker.

Other Samples
-------------

| Sample | What it covers? |
| ------ | --------------- |
| [Salesforce API Access](https://github.com/heroku-examples/heroku-integration-pattern-api-access-nodejs) | This sample application showcases how to extend a Heroku web application by integrating it with Salesforce APIs, enabling seamless data exchange and automation across multiple connected Salesforce orgs. It also includes a demonstration of the Salesforce Bulk API, which is optimized for handling large data volumes efficiently. |
| [Extending Apex, Flow and Agentforce](https://github.com/heroku-examples/heroku-integration-pattern-org-action-nodejs) | This sample demonstrates publishing a Heroku application into an org to enable Apex, Flow, and Agentforce to call out to Heroku. For Apex, both synchronous and asynchronous invocation are demonstrated, along with securely elevating Salesforce permissions for processing that requires additional object or field access. |
| [Scaling Batch Jobs with Heroku](https://github.com/heroku-examples/heroku-integration-pattern-org-job-nodejs) | This sample seamlessly delegates the processing of large amounts of data with significant compute requirements to Heroku Worker processes. It also demonstrates the use of the Unit of Work aspect of the SDK (JavaScript only for the pilot) for easier utilization of the Salesforce Composite APIs. |
| [Using Eventing to drive Automation and Communication](https://github.com/heroku-examples/heroku-integration-pattern-eventing-nodejs) | This sample extends the batch job sample by adding the ability to use eventing to start the work and notify users once it completes using Custom Notifications. These notifications are sent to the user's desktop or mobile device running Salesforce Mobile. Flow is used in this sample to demonstrate how processing can be handed off to low-code tools such as Flow. |
