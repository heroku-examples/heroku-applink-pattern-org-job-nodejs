'use strict';

// Helper function mirroring the Java example's discount logic
function getDiscountForRegion (region, logger) {
  // Basic discount logic based on region
  switch (region) {
    case 'NAMER':
      return 0.1; // 10%
    case 'EMEA':
      return 0.15; // 15%
    case 'APAC':
      return 0.08; // 8%
    default:
      return 0.05; // 5%
  }
}

/**
 * Helper function to fetch all records for a SOQL query, handling pagination.
 * @param {string} soql - The SOQL query string.
 * @param {object} sfContext - The initialized Salesforce context (ContextImpl instance).
 * @param {object} logger - A logger instance.
 * @returns {Promise<Array>} - A promise that resolves with an array of all records.
 */
async function queryAll (soql, sfContext, logger) {
  let allRecords = [];
  try {
    let result = await sfContext.org.dataApi.query(soql);
    allRecords = allRecords.concat(result.records);
    while (!result.done && result.nextRecordsUrl) {
      result = await sfContext.org.dataApi.queryMore(result); // Use result object directly
      allRecords = allRecords.concat(result.records);
    }
  } catch (error) {
    logger.error({ err: error, soql }, 'Error during queryAll execution');
    throw error; // Re-throw the error to be caught by the caller
  }
  return allRecords;
}

/**
 * Handles quote generation jobs.
 * @param {object} jobData - The job data object from Redis.
 * @param {object} sfContext - The initialized Salesforce context (ContextImpl instance).
 * @param {object} logger - A logger instance.
 */
async function handleQuoteMessage (jobData, sfContext, logger) {
  const { jobId, soqlWhereClause } = jobData;
  // Use soqlWhereClause in the query
  if (!soqlWhereClause) {
    logger.warn(`No soqlWhereClause provided for Job ID: ${jobId}`);
    return;
  }
  logger.info(`Worker received job with ID: ${jobId} for SOQL WHERE clause: ${soqlWhereClause}`);

  try {
    // Access APIs via sfContext.org
    if (!sfContext || !sfContext.org || !sfContext.org.dataApi) {
        logger.error(`Invalid sfContext or sfContext.org.dataApi for Quote Job ID: ${jobId}`);
        return;
    }
    const dataApi = sfContext.org.dataApi;

    // Fetch Standard Pricebook ID
    const standardPricebookRecords = await queryAll("SELECT Id FROM Pricebook2 WHERE IsStandard = true LIMIT 1", sfContext, logger);
    if (!standardPricebookRecords || standardPricebookRecords.length === 0) {
      logger.error(`Standard Pricebook not found for Job ID: ${jobId}.`);
      throw new Error('Standard Pricebook not found.');
    }
    const standardPricebookId = standardPricebookRecords[0].fields.Id || standardPricebookRecords[0].fields.id;

    // Query Opportunities
    const oppQuery = `
      SELECT Id, Name, AccountId, CloseDate, StageName, Amount,
             (SELECT Id, Product2Id, Quantity, UnitPrice, PricebookEntryId FROM OpportunityLineItems)
      FROM Opportunity
      WHERE ${soqlWhereClause}
    `; // Use the provided WHERE clause
    const opportunities = await queryAll(oppQuery, sfContext, logger);
    if (!opportunities || opportunities.length === 0) {
      logger.warn(`No Opportunities or related OpportunityLineItems found for WHERE clause: ${soqlWhereClause}`);
      return;
    }

    logger.info(`Processing ${opportunities.length} Opportunities`);
    const unitOfWork = dataApi.newUnitOfWork();
    const quoteRefs = new Map();
    let totalLineItems = 0;

    opportunities.forEach(oppSObject => {
      // Access fields using .fields property
      const opp = oppSObject.fields;
      const oppId = opp.Id || opp.id; // Get the actual ID
      // Access subquery results correctly
      const lineItemsResult = oppSObject.subQueryResults?.OpportunityLineItems;
      if (!lineItemsResult?.records || lineItemsResult.records.length === 0) {
        logger.warn(`Opportunity ${oppId} has no line items. Skipping quote creation for Job ID: ${jobId}`);
        return;
      }

      try {
        // 1. Create Quote
        const quoteName = 'New Quote';
        const expirationDate = new Date(opp.CloseDate);
        expirationDate.setDate(expirationDate.getDate() + 30); // Quote expires 30 days after CloseDate
        // Calculate discount based on hardcoded region (matching Java example 'US')
        const discount = getDiscountForRegion('NAMER', logger); // Use hardcoded region 'NAMER'
        const quoteRef = unitOfWork.registerCreate({
          type: 'Quote',
          fields: {
            Name: quoteName.substring(0, 80), // Ensure name is within limit
            OpportunityId: oppId,
            Pricebook2Id: standardPricebookId, // *** 3. Use fetched Standard Pricebook ID ***
            ExpirationDate: expirationDate.toISOString().split('T')[0],
            Status: 'Draft'
          }
        });
        quoteRefs.set(oppId, quoteRef);

        // 2. Create QuoteLineItems from OpportunityLineItems
        const currentOppLineItemCount = lineItemsResult.records.length;
        totalLineItems += currentOppLineItemCount;
        lineItemsResult.records.forEach(oliSObject => {
          // Apply discount to QuoteLineItem UnitPrice
          const oli = oliSObject.fields;
          const originalUnitPrice = oli.UnitPrice;
          const quantity = oli.Quantity;
          // Ensure discount is a number between 0 and 1
          const validDiscount = (typeof discount === 'number' && discount >= 0 && discount <= 1) ? discount : 0;
          const calculatedDiscountedPrice = (originalUnitPrice != null && validDiscount != null)
                                            ? originalUnitPrice * (1 - validDiscount)
                                            : originalUnitPrice; // Default to original if calculation fails
          unitOfWork.registerCreate({
            type: 'QuoteLineItem',
            fields: {
              QuoteId: quoteRef.toApiString(), // Reference the quote created above
              PricebookEntryId: oli.PricebookEntryId, // Must be valid PBE in the Quote's Pricebook
              Quantity: quantity,
              UnitPrice: calculatedDiscountedPrice // Use the calculated discounted price
            }
          });
        });
      } catch (err) {
        logger.error({ err: err, opportunityId: oppId }, `Error preparing UoW for Opportunity ${oppId} for Job ID: ${jobId}`);
      }
    });

    if (quoteRefs.size === 0) {
      logger.warn(`No quotes were registered for creation for Job ID: ${jobId}.`);
      return;
    }

    logger.info(`Submitting UnitOfWork to create ${quoteRefs.size} Quotes and ${totalLineItems} Line Items`);
    const commitResult = await dataApi.commitUnitOfWork(unitOfWork);

    // Process results
    let successCount = 0;
    let failureCount = 0;

    // Iterate through the original quoteRefs Map we created
    quoteRefs.forEach((originalQuoteRef, oppId) => {
      // Use the original reference object to get the result from the commit map
      const result = commitResult.get(originalQuoteRef);
      // Check for presence of id (success) or errors (failure)
      if (result?.id) { // Check if ID exists -> success
        successCount++;
      } else {
        failureCount++;
        // Log errors if they exist, otherwise log the whole result
        logger.error({ errors: result?.errors ?? result, opportunityId: oppId, refId: originalQuoteRef.id }, `Failed to create Quote for Opportunity ${oppId} (Ref ID: ${originalQuoteRef.id}) in Job ID: ${jobId}`);
      }
    });

    logger.info(`Job processing completed for Job ID: ${jobId}. Results: ${successCount} succeeded, ${failureCount} failed.`);

  } catch (error) {
    logger.error({ err: error }, `Error executing batch for Job ID: ${jobId}`);
  }
}

export {
  handleQuoteMessage
}; 