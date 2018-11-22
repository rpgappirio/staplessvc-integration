/*Platform Event trigger handler for inserting / updating Contacts
 * Created by Appirio 21 November 2018
 * The Platform Events Contact data interface replaces the existing CDW to SFDC daily batch integration mechanism
 * The integration pattern is based on the Staples target publish / subscribe itegration methodology where applications publish events to the ESB and 
 * subscribers can receive and process those events.
 * 
* The Contact data updates are based on Upsert pattern using ERP Number ID - system generated field that is a unique ID of the contact in SF. Concatenation of: ERP Number + "-" + Business Unit Code. Field is set by the interface. e.g. 96763-9000034

 */
public with sharing class PlatformEventUpdateContactTriggerHandler /* implements Database.Batchable<sobject> */ {
    final String PLATFORM_EVENT_TYPE = 'UPDATE CONTACT';

    public String handleAfterinsert(List<UpdateContact__e> events) {
        // Empty inits
        String errorString = '';
        List<Contact> recList = new List<Contact>();
        List<Platform_Event_Log__c> listLog = new List<Platform_Event_Log__c>();
        
        // PELog Start
        // TODO split into a class and logStep0 method for code generalization?
        try{ 
            for (UpdateContact__e event : events){
                Platform_Event_Log__c eventLogLine = new Platform_Event_Log__c (
                    Platform_Event__c=PLATFORM_EVENT_TYPE,
                    Success__c=FALSE,
                    Key__c=event.ERP_Number_ID__c,
                    // TODO: Add Status field to inflight
                    // TODO: Add number of retries defaulted to zero: do it on the object?
                    Event_Data__c='CONTACT EMAIL: '+event.Email__c
                    );
                listLog.add(eventLogLine);
            }
            Database.SaveResult[] res = Database.insert(listLog, false);
        } catch(DmlException e) {
            System.debug('The following exception has occurred: ' + e.getMessage());
        }

        // Data Mappings
        try {
            for(UpdateContact__e event : events) {
                // TODO: Split into a mapping class to make this code more generic?
                Contact rec = new Contact();
                
                // TODO: define nullable fields in spec
                if(!String.isBlank(event.Account_ERP_Number_ID__c)) rec.AccountId = new Contact(ERP_Number_ID__c=event.ERP_Number_ID__c).id;
                if(!String.isBlank(event.Business_Unit__c)) rec.Business_Unit__c = event.Business_Unit__c;
                if(!String.isBlank(event.ERP_Number__c)) rec.ERP_Number__c = event.ERP_Number__c;
                if(!String.isBlank(event.ERP_Number_ID__c)) rec.ERP_Number_ID__c = event.ERP_Number_ID__c;
                if(!String.isBlank(event.First_Name__c)) rec.First_Name__c = event.First_Name__c;
                if(!String.isBlank(event.Last_Name__c)) rec.Last_Name__c = event.Last_Name__c;
                if(!String.isBlank(event.Salutation__c)) rec.Salutation__c = event.Salutation__c;
                if(!String.isBlank(event.Title__c)) rec.Title = event.Title__c;
                if(!String.isBlank(event.Email__c)) rec.Email = event.Email__c;
                if(!String.isBlank(event.Business_Phone__c)) rec.Business_Phone__c = event.Business_Phone__c;
                if(!String.isBlank(event.Mobile_Phone__c)) rec.MobilePhone = event.Mobile_Phone__c;
                if(!String.isBlank(event.Other_Phone__c)) rec.OtherPhone = event.Other_Phone__c;
                if(!String.isBlank(event.Home_Phone__c)) rec.HomePhone = event.Home_Phone__c;
                
                recList.add(rec);
            } 

            // Upserting data
            if(!recList.isEmpty()) {
                // TODO: Batch?
                Database.UpsertResult[] sr = Database.upsert(recList, Contact.ERP_Number_ID__c);
                for(Integer i = 0, size = sr.size(); i < size; i++) {
                    if(sr[i].isSuccess()) {
                        if(sr[i].isCreated()) {System.debug(recList[i].ERP_Number_ID__c +' was created');} 
                        else {System.debug(recList[i].ERP_Number_ID__c +' was updated');}

                        // TODO split into a class and logStep0 method for code generalization?
                        // Create Platform Event Log record for success
                        Platform_Event_Log__c event = new Platform_Event_Log__c (
                                        Platform_Event__c= PLATFORM_EVENT_TYPE,
                                        Success__c=TRUE,
                                        Key__c=recList[i].ERP_Number_ID__c,
                                        // TODO: Add Status field to processed
                                        Event_Data__c='CONTACT EMAIL: '+recList[i].Email
                                        // TODO: add creation or update of account?
                                        );
                                        
                        // add the Event Log record to the list of Event Logs to be inserted
                        listLog.add(event);
                    }
                    // Upsert result is error 
                    else {
                        String logLineErrorMessage= '[ERROR] Account Name: ' + recList[i].Name + ' (' + sr[i].getErrors().size() + ' errors)\n';
                        for(Database.Error err : sr[i].getErrors()) {
                            // Send a failure back to the Trigger and retry using EventBus.RetryableException. See https://developer.salesforce.com/docs/atlas.en-us.platform_events.meta/platform_events/platform_events_subscribe_apex_refire.htm
                            logLineErrorMessage+= '   Fault '+i+': '+err.getStatusCode()+' - '+ err.getMessage()+'. Fields: '+err.getFields()+'\n';
                            System.debug('error has occurred on' + recList[i].Name +': '+ err.getStatusCode() + ': ' + err.getMessage());                    
                            System.debug('fields that affected this error: ' + err.getFields());
                        }   
                        // Create Platform Event Log record for failure
                        // TODO split into a class and logStep0 method for code generalization?
                        Platform_Event_Log__c event = new Platform_Event_Log__c (
                                        Platform_Event__c=PLATFORM_EVENT_TYPE,
                                        Success__c=FALSE,
                                        Key__c=recList[i].ERP_Number_ID__c,
                                        // TODO: Add Status field to inflight-retry
                                        Event_Data__c='CONTACT EMAIL: '+recList[i].Email,
                                        Error_Message__c=logLineErrorMessage
                                        );
                                        
                        // add the Event Log record to the list of Event Logs to be inserted
                        listLog.add(event);

                        // prepare the errorString for retry
                        errorString+= logLineErrorMessage;
                    }
                }
            }
            // Insert the Platform Event Logs
            // TODO: Batch?
            Database.SaveResult[] res = Database.update(listLog, false);
            // TODO process PElog result into a debug log or related? alert?
        } catch(Exception e) {
            errorString+= e.getMessage();
            if(errorString.length() > 255) {return errorString.substring(0,255);}
            else return e.getMessage() + errorString;
        }
        
		if(errorString.length() > 255) {return errorString.substring(0,255);}
        else return errorString;
    }
}