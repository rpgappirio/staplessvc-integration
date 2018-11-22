/*Platform Event trigger for inserting / updating Contacts
 * Created by Appirio 21 November 2018
 * The Platform Events Contact data interface replaces the existing CDW to SFDC daily batch integration mechanism
 * The integration pattern is based on the Staples target publish / subscribe itegration methodology where applications publish events to the ESB and 
 * subscribers can receive and process those events.
 * 
* The Contact data updates are based on Upsert pattern using ERP Number ID - system generated field that is a unique ID of the contact in SF. Concatenation of: ERP Number + "-" + Business Unit Code. Field is set by the interface. e.g. 96763-9000034

 */

 // after insert only
trigger PlatformEventUpdateContactTrigger on Contact (after insert) {
        String errorString = '';
        Integer MAX_RETRY_COUNT= 10;
    
    try {
        // Call Object-specific Trigger Handler
        PlatformEventUpdateContactTriggerHandler handler = new PlatformEventUpdateContactTriggerHandler();
        errorString = handler.handleAfterinsert(Trigger.new);
        
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