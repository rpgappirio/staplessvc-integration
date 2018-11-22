/*Platform Event trigger for inserting / updating Accouts
 * Created by Appirio 21 November 2018
 * The Platform Events Account data interace replaces the existing CDW to SFDC daily batch integration mechanism
 * The integration pattern is based on the Staples target publish / subscribe itegration methodology where applications publish events to the ESB and 
 * subscribers can receive and process those events.
 * 
 * The Account data updates are based on Upsert pattern using the External_ID ' ERP_Number_ID__c as the key using format Business Unit Code + '-' + ERP Number
 */

// after insert only
trigger PlatformEventUpdateAccountTrigger on UpdateAccount__e (after insert) {
    String errorString = '';
    Integer MAX_RETRY_COUNT= 10;
    
    try {
        // Call Object-specific Trigger Handler
        PlatformEventUpdateAccountTriggerHandler handler = new PlatformEventUpdateAccountTriggerHandler();
        errorString = handler.handleAfterInsert(Trigger.new);
        
        // Retry logic
        if(errorString != '') {
            // TODO - retry logic entry criteria needed? thinking on execution limits
            if(EventBus.TriggerContext.currentContext().retries < MAX_RETRY_COUNT) {
                System.debug('Error has occurred. Retry attempt ' + EventBus.TriggerContext.currentContext().retries + ' ' + errorString);
                
                throw new EventBus.RetryableException('in count');
            } else {
                System.debug('Error has occurred. Retry attempt last ('+MAX_RETRY_COUNT+'): ' + errorString);
            }
        }
        
        // TODO: One of the issues we face with Platform Events is you can't query the event objects directly so difficult to know what's happening. We've created a custom object for logging each transactions
    } catch(DmlException e) {
        // TODO
    	System.debug('The following exception has occurred: ' + e.getMessage());
    }
}