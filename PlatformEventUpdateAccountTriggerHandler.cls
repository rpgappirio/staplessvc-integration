/*Platform Event trigger handler for inserting / updating Accounts
 * Created by Appirio 21 November 2018
 * The Platform Events Account data interface replaces the existing CDW to SFDC daily batch integration mechanism
 * The integration pattern is based on the Staples target publish / subscribe itegration methodology where applications publish events to the ESB and 
 * subscribers can receive and process those events.
 * 
 * The Account data updates are based on Upsert pattern using the External_ID ' ERP_Number_ID__c as the key using format Business Unit Code + '-' + ERP Number
 */
public with sharing class PlatformEventUpdateAccountTriggerHandler /* implements Database.Batchable<sobject> */ {
    final String PLATFORM_EVENT_TYPE = 'UPDATE ACCOUNT';

    public String handleAfterinsert(List<UpdateAccount__e> events) {
        // Empty inits
        String errorString = '';
        List<Account> recList = new List<Account>();
        List<Platform_Event_Log__c> listLog = new List<Platform_Event_Log__c>();
        
        // PELog Start
        // TODO split into a class and logStep0 method for code generalization?
        try{ 
            for (UpdateAccount__e event : events){
                Platform_Event_Log__c eventLogLine = new Platform_Event_Log__c (
                    Platform_Event__c=PLATFORM_EVENT_TYPE,
                    Success__c=FALSE,
                    Key__c=event.ERP_Number_ID__c,
                    // TODO: Add Status field to inflight
                    // TODO: Add number of retries defaulted to zero: do it on the object?
                    Event_Data__c='ACCOUNT NAME: '+event.Account_Name__c
                    );
                listLog.add(eventLogLine);
            }
            Database.SaveResult[] res = Database.insert(listLog, false);
        } catch(DmlException e) {
            System.debug('The following exception has occurred: ' + e.getMessage());
        }
        
        // Data Mappings
        try {
            for(UpdateAccount__e event : events) {
                // TODO: Split into a mapping class to make this code more generic?
                Account rec = new Account();
                
                // TODO: define nullable fields in spec
                if(!String.isBlank(event.ERP_Number_ID__c)) rec.ERP_Number_ID__c = event.ERP_Number_ID__c;
                if(!String.isBlank(event.Parent_Account_ERP_Number_ID__c)) rec.parentId =  new Account(ERP_Number_ID__c=event.Parent_Account_ERP_Number_ID__c).id;
                if(!String.isBlank(event.Account_Name__c)) rec.Name = event.Account_Name__c;
                if(!String.isBlank(event.Account_Owner_in_ERP__c)) rec.Account_Owner_in_ERP__c = event.Account_Owner_in_ERP__c;
                if(!String.isBlank(event.Billing_City__c)) rec.BillingCity = event.Billing_City__c;
                if(!String.isBlank(event.Billing_Street__c) )rec.BillingStreet = event.Billing_Street__c;
                if(!String.isBlank(event.Billing_Postal_Code__c)) rec.BillingPostalCode = event.Billing_Postal_Code__c;
                if(!String.isBlank(event.Billing_Country__c)) rec.BillingCountry = event.Billing_Country__c;
                if(!String.isBlank(event.Phone__c)) rec.Phone = event.Phone__c;
                if(!String.isBlank(event.Business_Unit__c)) rec.Business_Unit__c = event.Business_Unit__c;
                if(!String.isBlank(event.Distribution_Channel__c)) rec.Distribution_Channel__c = event.Distribution_Channel__c;
                if(!String.isBlank(event.ERP_Account_Status__c)) rec.ERP_Account_Status__c = event.ERP_Account_Status__c;
                if(!String.isBlank(event.ERP_Search_Key__c)) rec.ERP_Search_Key__c = event.ERP_Search_Key__c;
                if(!String.isBlank(event.ERP_Customer_Group__c)) rec.ERP_Customer_Group__c = event.ERP_Customer_Group__c;
                if(!String.isBlank(event.ERP_Level__c)) rec.ERP_Level__c = event.ERP_Level__c;
                if(!String.isBlank(event.ERP_Number__c)) rec.ERP_Number__c = event.ERP_Number__c;
                if(!String.isBlank(event.ERP_Search_2__c)) rec.ERP_Search_2__c = event.ERP_Search_2__c;
                if(!String.isBlank(event.ERP_Search_3__c)) rec.ERP_Search_3__c = event.ERP_Search_3__c;
                if(!String.isBlank(event.Parent_Account_ERP_Number_ID__c)) rec.ParentId = event.Parent_Account_ERP_Number_ID__c;
                if(!String.isBlank(event.Segment__c)) rec.Segment__c = event.Segment__c;
                if(!String.isBlank(event.VAT_Number__c)) rec.VAT_Number__c = event.VAT_Number__c;
                if(!String.isBlank(event.Web_Account_Type__c)) rec.Web_Account_Type__c = event.Web_Account_Type__c;
                
                recList.add(rec);
            } 

            // Upserting data
            if(!recList.isEmpty()) {
                // TODO: Batch?
                Database.UpsertResult[] sr = Database.upsert(recList, Account.ERP_Number_ID__c);
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
                                        Event_Data__c='ACCOUNT NAME: '+recList[i].Name
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
                                        // TODO: Add Status field to inflight
                                        Event_Data__c='ACCOUNT NAME: '+recList[i].Name,
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