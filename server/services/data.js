'use strict';

// --- Bulk API Helper Constants ---
const BULK_API_POLL_INTERVAL = 5000; // 5 seconds
const BULK_API_TIMEOUT = 300000; // 5 minutes

// --- Data Generation Helpers ---

function generateSampleOpportunities (count, accountId, pricebookId) {
  const opportunities = [];
  for (let i = 0; i < count; i++) {
    opportunities.push({
      // Ensure required fields are included
      Name: `Sample Opp ${Date.now()}-${i}`,
      AccountId: accountId, // Need a valid Account ID
      StageName: 'Prospecting',
      CloseDate: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0], // ~30 days from now
      Pricebook2Id: pricebookId
    });
  }
  return opportunities;
}

function generateSampleOLIs (createdOppIds, pricebookEntries) {
  const olis = [];
  const FIXED_OLI_COUNT = 2;
  if (!createdOppIds || createdOppIds.length === 0 || !pricebookEntries || pricebookEntries.length === 0) {
    return olis;
  }

  createdOppIds.forEach(oppId => {
    // Add FIXED_OLI_COUNT line items for each opp
    for (let i = 0; i < FIXED_OLI_COUNT; i++) {
      // Pick a random PBE ensuring we don't exceed available entries
      const entryIndex = Math.floor(Math.random() * pricebookEntries.length);
      const pbe = pricebookEntries[entryIndex];

      // Ensure we got a valid PBE object with Id and Product2Id
      if (pbe && pbe.Id && pbe.Product2Id) {
          olis.push({
            OpportunityId: oppId,
            PricebookEntryId: pbe.Id,
            Product2Id: pbe.Product2Id, // Include Product2Id
            Quantity: Math.floor(Math.random() * 10) + 1,
            UnitPrice: Math.floor(Math.random() * 100) + 10 // Random price between 10-110
          });
      } else {
          // Log a warning if a valid PBE couldn't be found for this iteration
          // Use console.warn directly as logger might not be available here
          console.warn(`[DataService][DataGen] Could not find valid PricebookEntry with Product2Id for iteration ${i} on Opp ${oppId}. Skipping OLI.`);
      }
    }
  });
  return olis;
}

// --- Bulk API Polling Helper ---

async function pollBulkJobStatus (jobReference, bulkApi, logger) {
  let jobInfo;
  const startTime = Date.now();

  const jobId = jobReference.id;
  logger.info(`[DataService][BulkAPI] Polling status for Job ID: ${jobId} (Type: ${jobReference.type})`);

  while (Date.now() - startTime < BULK_API_TIMEOUT) {
    try {
      jobInfo = await bulkApi.getInfo(jobReference);

      if (!jobInfo) {
          logger.error(`[DataService][BulkAPI] bulkApi.getInfo(${jobId}) returned undefined.`);
          throw new Error(`bulkApi.getInfo(${jobId}) returned undefined.`);
      }

      logger.debug(`[DataService][BulkAPI] Job ${jobId} status: ${jobInfo.state}`);

      if (jobInfo.state === 'JobComplete') {
        logger.info(`[DataService][BulkAPI] Job ${jobId} processing complete.`);
        return jobInfo; // Success
      } else if (jobInfo.state === 'Failed' || jobInfo.state === 'Aborted') {
        logger.error(`[DataService][BulkAPI] Job ${jobId} failed or was aborted. State: ${jobInfo.state}, Message: ${jobInfo.errorMessage}`);
        throw new Error(`Bulk API Job ${jobId} failed or aborted: ${jobInfo.state}`);
      }
    } catch (err) {
      logger.error({ err: err }, `[DataService][BulkAPI] Error polling job ${jobId}`);
      throw err; // Rethrow error after logging
    }

    await new Promise(resolve => setTimeout(resolve, BULK_API_POLL_INTERVAL));
  }

  // Timeout reached
  logger.error(`[DataService][BulkAPI] Timeout polling job ${jobId}. Last state: ${jobInfo?.state}`);
  throw new Error(`Timeout polling Bulk API Job ${jobId}`);
}

// --- Main Data Handler ---

/**
 * Handles data creation or deletion jobs.
 * @param {object} jobData - The job data object from Redis.
 * @param {object} sfContext - The initialized Salesforce context (ContextImpl instance).
 * @param {object} logger - A logger instance.
 */
async function handleDataMessage (jobData, sfContext, logger) {
  logger.info(`[DataService] Handling data job object`);

  const { jobId: processJobId, operation, count = 10 } = jobData;
  // Note: context is no longer destructured here, sfContext is passed in

  try {
    // *** Access APIs via sfContext.org ***
    if (!sfContext || !sfContext.org || !sfContext.org.dataApi || !sfContext.org.bulkApi) {
        logger.error(`[DataService] Invalid sfContext or missing APIs for Data Job ID: ${processJobId}`);
        return;
    }
    const dataApi = sfContext.org.dataApi;
    const bulkApi = sfContext.org.bulkApi;

    logger.info(`[DataService] Processing Data Job ID: ${processJobId}, Operation: ${operation}, Count: ${count}`);

    if (operation === 'create') {
      logger.info(`[DataService] Starting data creation via Bulk API for Job ID: ${processJobId}, Count: ${count}`);

      // 1. Prerequisites (use dataApi)
      logger.info(`[DataService] Fetching prerequisites for Job ID: ${processJobId}`);
      let accounts;
      try {
        logger.info('[DataService] Attempting: dataApi.query("SELECT Id FROM Account LIMIT 1")');
        accounts = await dataApi.query("SELECT Id FROM Account LIMIT 1");
      } catch (queryError) {
        logger.error({ err: queryError }, '[DataService] Error during dataApi.query for Account');
        throw queryError;
      }

      if (!(accounts?.records?.[0]?.fields?.Id || accounts?.records?.[0]?.fields?.id)) {
          const reason = !accounts ? 'Query result is null/undefined'
                       : !accounts.records ? 'Query result missing \'records\' property'
                       : accounts.records.length === 0 ? 'Query returned 0 records'
                       : 'First record missing fields.Id/fields.id property';
          logger.error(`[DataService] No Account found. Reason: ${reason}.`);
          throw new Error(`No Account found. Reason: ${reason}`);
      }
      const accountId = accounts.records[0].fields.Id || accounts.records[0].fields.id;
      logger.info(`[DataService] Using Account ID: ${accountId} for Job ID: ${processJobId}`);

      const standardPricebookResult = await dataApi.query("SELECT Id FROM Pricebook2 WHERE IsStandard = true LIMIT 1");
      if (!(standardPricebookResult?.records?.[0]?.fields?.Id || standardPricebookResult?.records?.[0]?.fields?.id)) { throw new Error('Standard Pricebook not found or missing Id.'); }
      const standardPricebookId = standardPricebookResult.records[0].fields.Id || standardPricebookResult.records[0].fields.id;

      const pbes = await dataApi.query(`SELECT Id, Product2Id FROM PricebookEntry WHERE Pricebook2Id = '${standardPricebookId}' AND IsActive = true LIMIT 10`);
      if (!pbes?.records || pbes.records.length === 0) { throw new Error('No active Pricebook Entries found.'); }
      const pricebookEntries = pbes.records.map(pbe => pbe?.fields).filter(pbe => pbe && pbe.Id && pbe.Product2Id);
      if (pricebookEntries.length === 0) { throw new Error('No valid Pricebook Entries with Product2Id found.'); }
      logger.info(`[DataService] Found ${pricebookEntries.length} valid PBEs from Standard Pricebook for Job ID: ${processJobId}`);

      // --- Create Opportunities via Bulk API (use bulkApi) ---
      logger.info(`[DataService][BulkAPI] Preparing Opportunity creation job for Job ID: ${processJobId}`);
      const oppsToCreate = generateSampleOpportunities(count, accountId, standardPricebookId);

      const oppColumns = Object.keys(oppsToCreate[0] || {});
      const oppDataTable = oppsToCreate.map(opp => {
          const rowMap = new Map();
          oppColumns.forEach(col => rowMap.set(col, opp[col]));
          return rowMap;
      });
      oppDataTable.columns = oppColumns;

      const oppIngestResult = await bulkApi.ingest({ object: 'Opportunity', operation: 'insert', dataTable: oppDataTable });

      let oppJobReference;
      if (Array.isArray(oppIngestResult) && oppIngestResult[0]?.error) {
          logger.error({ errorDetails: oppIngestResult[0].error }, `[DataService][BulkAPI] bulkApi.ingest for Opportunities failed.`);
          throw new Error(`bulkApi.ingest for Opportunities failed.`);
      } else if (Array.isArray(oppIngestResult) && oppIngestResult[0]?.id && oppIngestResult[0]?.type) {
          oppJobReference = oppIngestResult[0];
      } else {
          logger.error({ oppIngestResult }, `[DataService][BulkAPI] bulkApi.ingest for Opportunities returned unexpected structure.`);
          throw new Error('bulkApi.ingest for Opportunities returned unexpected structure.');
      }

      logger.info(`[DataService][BulkAPI] Submitted Opportunity creation job with ID: ${oppJobReference.id} for main Job ID: ${processJobId}`);
      const oppJobInfo = await pollBulkJobStatus(oppJobReference, bulkApi, logger);
      logger.info(`[DataService][BulkAPI] Opp job ${oppJobReference.id} completed. State: ${oppJobInfo.state}, Processed: ${oppJobInfo.numberRecordsProcessed}, Failed: ${oppJobInfo.numberRecordsFailed}`);

      if (oppJobInfo.numberRecordsFailed > 0) {
          try {
              const failedRecords = await bulkApi.getFailedResults(oppJobReference);
              logger.warn(`[DataService][BulkAPI] Opportunity creation job ${oppJobReference.id} had ${oppJobInfo.numberRecordsFailed} failures. Details:`, failedRecords);
          } catch(failErr) {
              logger.error({err: failErr}, `[DataService][BulkAPI] Error fetching failed results for job ${oppJobReference.id}`);
          }
      }

      if (oppJobInfo.numberRecordsProcessed === 0 || oppJobInfo.numberRecordsProcessed === oppJobInfo.numberRecordsFailed) {
          logger.error(`[DataService][BulkAPI] No Opportunities successfully created by job ${oppJobReference.id}. Aborting OLI creation for Job ID: ${processJobId}.`);
          return;
      }

      // --- Create OLIs via Bulk API (use bulkApi) ---
      logger.info(`[DataService][BulkAPI] Fetching successful results for Opportunity job ${oppJobReference.id}`);
      let successfulOppIds = [];
       try {
            const successfulRecords = await bulkApi.getSuccessfulResults(oppJobReference);
            successfulOppIds = successfulRecords.map(rec => rec.get('sf__Id')).filter(id => id);
            logger.info(`[DataService][BulkAPI] Extracted ${successfulOppIds.length} successful Opportunity IDs for Job ID: ${processJobId}`);
       } catch(successErr) {
           logger.error({err: successErr}, `[DataService][BulkAPI] Error fetching successful results for Opportunity job ${oppJobReference.id}. Cannot create OLIs.`);
           return;
       }

       if (successfulOppIds.length === 0) {
            logger.warn(`[DataService][BulkAPI] No successful Opportunity IDs retrieved from job ${oppJobReference.id}. Cannot create OLIs for Job ID: ${processJobId}.`);
            return;
       }

      logger.info(`[DataService][BulkAPI] Preparing OLI creation job for ${successfulOppIds.length} Opportunities for Job ID: ${processJobId}`);
      const olisToCreate = generateSampleOLIs(successfulOppIds, pricebookEntries);

      if (olisToCreate.length === 0) {
          logger.info(`[DataService][BulkAPI] No OLIs generated. Skipping OLI creation job for Job ID: ${processJobId}`);
      } else {
          const oliColumns = Object.keys(olisToCreate[0] || {});
          const oliDataTable = olisToCreate.map(oli => {
              const rowMap = new Map();
              oliColumns.forEach(col => rowMap.set(col, oli[col]));
              return rowMap;
          });
          oliDataTable.columns = oliColumns;

          const oliIngestResult = await bulkApi.ingest({ object: 'OpportunityLineItem', operation: 'insert', dataTable: oliDataTable });

          let oliJobReference;
          if (Array.isArray(oliIngestResult) && oliIngestResult[0]?.error) {
              logger.error({ errorDetails: oliIngestResult[0].error }, `[DataService][BulkAPI] bulkApi.ingest for OLIs failed.`);
              logger.warn(`[DataService][BulkAPI] OLI creation job submission failed.`);
          } else if (Array.isArray(oliIngestResult) && oliIngestResult[0]?.id && oliIngestResult[0]?.type) {
              oliJobReference = oliIngestResult[0];
          } else {
              logger.error({ oliIngestResult }, `[DataService][BulkAPI] bulkApi.ingest for OLIs returned unexpected structure.`);
              logger.warn(`[DataService][BulkAPI] OLI creation job submission returned unexpected structure.`);
          }

          if (oliJobReference) {
              logger.info(`[DataService][BulkAPI] Submitted OLI creation job with ID: ${oliJobReference.id} for main Job ID: ${processJobId}`);
              const oliJobInfo = await pollBulkJobStatus(oliJobReference, bulkApi, logger);
              logger.info(`[DataService][BulkAPI] OLI job ${oliJobReference.id} completed. State: ${oliJobInfo.state}, Processed: ${oliJobInfo.numberRecordsProcessed}, Failed: ${oliJobInfo.numberRecordsFailed}`);
              if (oliJobInfo.numberRecordsFailed > 0) {
                  try {
                     const failedRecords = await bulkApi.getFailedResults(oliJobReference);
                     logger.warn(`[DataService][BulkAPI] OLI creation job ${oliJobReference.id} had ${oliJobInfo.numberRecordsFailed} failures. Details:`, failedRecords);
                  } catch(failErr) {
                     logger.error({err: failErr}, `[DataService][BulkAPI] Error fetching failed results for OLI job ${oliJobReference.id}`);
                  }
              }
          } else {
             logger.warn(`[DataService][BulkAPI] Skipping OLI job polling because submission failed or returned invalid reference.`);
          }
      }

      logger.info(`[DataService][BulkAPI] Completed data creation process for Job ID: ${processJobId}`);

    } else if (operation === 'delete') {
      logger.info(`[DataService] Starting data deletion via Bulk API for Job ID: ${processJobId}`);

      // --- Delete Operation via Bulk API ---
      const MAX_DELETE_QUERY = 5000;
      const oppsToDeleteQuery = `SELECT Id FROM Opportunity WHERE Name LIKE 'Sample Opp %' LIMIT ${MAX_DELETE_QUERY}`;
      logger.info(`[DataService][BulkAPI] Querying up to ${MAX_DELETE_QUERY} Opportunities for deletion: ${oppsToDeleteQuery} for Job ID: ${processJobId}`);
      const oppsToDeleteResult = await dataApi.query(oppsToDeleteQuery);

      if (!oppsToDeleteResult?.records || oppsToDeleteResult.records.length === 0) {
        logger.info(`[DataService][BulkAPI] No sample Opportunities found to delete for Job ID: ${processJobId}`);
        return;
      }
      const oppIdsToDelete = oppsToDeleteResult.records.map(opp => ({ Id: opp?.fields?.Id || opp?.fields?.id })).filter(item => item.Id);
      logger.info(`[DataService][BulkAPI] Found ${oppIdsToDelete.length} Opportunities to delete for Job ID: ${processJobId}`);

      logger.info(`[DataService][BulkAPI] Preparing Opportunity deletion job for Job ID: ${processJobId}`);

       const deleteColumns = ['Id'];
       const deleteDataTable = oppIdsToDelete.map(opp => {
           const rowMap = new Map();
           deleteColumns.forEach(col => rowMap.set(col, opp[col]));
           return rowMap;
       });
       deleteDataTable.columns = deleteColumns;

      const deleteIngestResult = await bulkApi.ingest({ object: 'Opportunity', operation: 'hardDelete', dataTable: deleteDataTable });

      let deleteJobReference;
      if (Array.isArray(deleteIngestResult) && deleteIngestResult[0]?.error) {
          logger.error({ errorDetails: deleteIngestResult[0].error }, `[DataService][BulkAPI] bulkApi.ingest for Deletion failed.`);
          throw new Error(`bulkApi.ingest for Deletion failed.`);
      } else if (Array.isArray(deleteIngestResult) && deleteIngestResult[0]?.id && deleteIngestResult[0]?.type) {
          deleteJobReference = deleteIngestResult[0];
      } else {
          logger.error({ deleteIngestResult }, `[DataService][BulkAPI] bulkApi.ingest for Deletion returned unexpected structure.`);
          throw new Error('bulkApi.ingest for Deletion returned unexpected structure.');
      }

      logger.info(`[DataService][BulkAPI] Submitted Deletion job with ID: ${deleteJobReference.id} for main Job ID: ${processJobId}`);
      const deleteJobInfo = await pollBulkJobStatus(deleteJobReference, bulkApi, logger);
      logger.info(`[DataService][BulkAPI] Deletion job ${deleteJobReference.id} completed. State: ${deleteJobInfo.state}, Processed: ${deleteJobInfo.numberRecordsProcessed}, Failed: ${deleteJobInfo.numberRecordsFailed}`);
       if (deleteJobInfo.numberRecordsFailed > 0) {
           try {
              const failedRecords = await bulkApi.getFailedResults(deleteJobReference);
              logger.warn(`[DataService][BulkAPI] Deletion job ${deleteJobReference.id} had ${deleteJobInfo.numberRecordsFailed} failures. Details:`, failedRecords);
           } catch(failErr) {
              logger.error({err: failErr}, `[DataService][BulkAPI] Error fetching failed results for deletion job ${deleteJobReference.id}`);
           }
       }

      logger.info(`[DataService][BulkAPI] Completed data deletion process for Job ID: ${processJobId}`);

    } else {
      logger.warn(`[DataService] Unknown data operation requested: ${operation} for Job ID: ${processJobId}`);
    }

  } catch (error) {
    logger.error({ err: error }, `[DataService] Critical error processing Data Job ID ${processJobId}`);
  }
}

export {
  handleDataMessage
}; 