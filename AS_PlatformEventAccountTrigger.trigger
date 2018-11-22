/* Platform Event trigger handler for inserting / updating Accounts.
** Created by Appirio 08 October 2018
** The Platform Events Account data interface replaces the existing CDW to SFDC daily batch integration mechanism. 
** This integration pattern is based on the Staples target publish / subscribe integration methodology where applications publish events to the Enterprise Service Bus and
** subscibers can receive and process those events. 
**
** The Account data updates are based on Upsert pattern using the External ID 'ERP_Number_ID__c' as the key using format: Business Unit Code + "-" + ERP Number. 
** This interface allows the ERP applications to publish account data updates (inserts / updates) by inserting a Platform Event. 
** The Event handler (trigger) on SFDC processes the updates to the Account data.  Platform Event trigger fire on Insert only.
** See Platform Events Developer Guide https://resources.docs.salesforce.com/216/latest/en-us/sfdc/pdf/platform_events.pdf
**/
trigger AS_PlatformEventAccountTrigger on Update_Account__e (after insert) {
    // Process Account records
    String errorString = '';
    // Call the handler
    AS_PlatformEventAccountTriggerHandler handler = new AS_PlatformEventAccountTriggerHandler ();
    
    errorString = handler.handleAfterInsert(Trigger.new);
    
    
    // Retry Event Triggers with EventBus.RetryableException in case the trigger failed. Primary reason for failure being the Parent Account doesn't exist
    if (errorString != '') {
        // Retry the trigger up to 9 times
        if (EventBus.TriggerContext.currentContext().retries < 10) {
            // Condition isn't met, so try again later.
            System.debug('Error has occurred. Retry attempt ' + EventBus.TriggerContext.currentContext().retries + ': ' + errorString  ); 
            
            // Condition isn't met, so try again later.
            throw new EventBus.RetryableException(
                     'Condition is not met, so retrying the trigger again.');
                     
        } else {
            // Trigger was retried max number of times so giving up.
            System.debug('Error has occurred. EventBus.RetryableException retried 10 times, now giving up. ' + errorString  );                    
                        
        }
    }
    
}