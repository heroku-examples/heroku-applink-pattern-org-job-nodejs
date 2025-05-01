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
 * Handles quote generation jobs.
 * @param {object} jobData - The job data object from Redis.
 * @param {object} sfContext - The initialized Salesforce context (ContextImpl instance).
 * @param {object} logger - A logger instance.
 */
async function handleQuoteMessage (jobData, sfContext, logger) {
  // *** 1. Use soqlWhereClause instead of opportunityIds ***
  const { jobId, soqlWhereClause } = jobData;
  // Note: context is no longer destructured here, sfContext is passed in
  logger.info(`Worker received job with ID: ${jobId} for SOQL WHERE clause: ${soqlWhereClause}`);

  try {
    // *** Access APIs via sfContext.org ***
    if (!sfContext || !sfContext.org || !sfContext.org.dataApi) {
        logger.error(`Invalid sfContext or sfContext.org.dataApi for Quote Job ID: ${jobId}`);
        return;
    }
    const dataApi = sfContext.org.dataApi;

    logger.info(`Worker executing batch for Job ID: ${jobId} with WHERE clause: ${soqlWhereClause}`);

    // *** 2. Fetch Standard Pricebook ID ***
    const standardPricebookResult = await dataApi.query("SELECT Id FROM Pricebook2 WHERE IsStandard = true LIMIT 1");
    if (!(standardPricebookResult?.records?.[0]?.fields?.Id || standardPricebookResult?.records?.[0]?.fields?.id)) {
        logger.error(`Standard Pricebook not found for Job ID: ${jobId}.`);
        throw new Error('Standard Pricebook not found.');
    }
    const standardPricebookId = standardPricebookResult.records[0].fields.Id || standardPricebookResult.records[0].fields.id;

    // *** 1. Use soqlWhereClause in the query ***
    if (!soqlWhereClause) {
      logger.warn(`No soqlWhereClause provided for Job ID: ${jobId}`);
      return;
    }
    const oppQuery = `
      SELECT Id, Name, AccountId, CloseDate, StageName, Amount,
             (SELECT Id, Product2Id, Quantity, UnitPrice, PricebookEntryId FROM OpportunityLineItems)
      FROM Opportunity
      WHERE ${soqlWhereClause}
    `; // Use the provided WHERE clause
    const oppResult = await dataApi.query(oppQuery);
    const opportunities = oppResult.records;
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
        const quoteName = `Quote for ${opp.Name} - ${new Date().toISOString().split('T')[0]}`;
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
            // *** 4. Remove Discount field from Quote ***
          }
        });
        quoteRefs.set(oppId, quoteRef);

        // 2. Create QuoteLineItems from OpportunityLineItems
        const currentOppLineItemCount = lineItemsResult.records.length;
        totalLineItems += currentOppLineItemCount;
        lineItemsResult.records.forEach(oliSObject => {
          // Access fields using .fields property
          const oli = oliSObject.fields;

          // *** 4. Apply discount to QuoteLineItem UnitPrice ***
          const originalUnitPrice = oli.UnitPrice;
          const quantity = oli.Quantity;
          // Ensure discount is a number between 0 and 1
          const validDiscount = (typeof discount === 'number' && discount >= 0 && discount <= 1) ? discount : 0;
          // Check for null/undefined before calculation
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

    // *** Iterate through the original quoteRefs Map we created ***
    quoteRefs.forEach((originalQuoteRef, oppId) => {
      // *** Use the original reference object to get the result from the commit map ***
      const result = commitResult.get(originalQuoteRef);
      // *** Check for presence of id (success) or errors (failure) ***
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