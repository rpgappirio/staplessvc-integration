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

public with sharing class AS_PlatformEventAccountTriggerHandler {
    
    // After Insert
    public String handleAfterInsert ( List<Update_Account__e> accountEvents ) {
    
        // Return statement to send back to the trigger
        String errorString = '';
        // List to hold all accounts to be upserted.
        List<Account> listAccounts = new List<Account>();
        // List of Account external ids to query parent accounts
        List<String> parentAccountExternalIds = new List<String>();
        // Parent Account record
        Account parentAccount;
        // List to hold all Parent accounts to be inserted.
        List<Account> listParentAccounts = new List<Account>();
        // List of Event Logs
        List<Platform_Event_Log__c> listLog = new List<Platform_Event_Log__c>();
        
        
        // Iterate through each event notification
        for (Update_Account__e event : accountEvents) {
            System.debug('Got upsert event for account: ' + event);
            
            // Create the Parent and Child Accounts based on the External ID's
            if (event.Parent_Account_ERP_Number_ID__c != null && event.Parent_Account_ERP_Number_ID__c != '') {
                
                // Get a reference to the parent Account based on the event.Parent_Account_ERP_Number_ID__c. This method assumes the Parent Account already exists.
                // It is used to create or update the account and set the parentId in a single DML insert or update.  If the Parent Account does not exist, 
                // identified by external id event.Parent_Account_ERP_Number_ID__c, then the DML statement fails.
                parentAccount = new Account(ERP_Number_ID__c=event.Parent_Account_ERP_Number_ID__c);
                
                // Create or update the child Account record and relate to the ParentAccount
                Account account = new Account(
                                    Name=event.Account_Name__c,
                                    Account_Owner_in_ERP__c = event.Account_Owner_in_ERP__c,
                                    BillingCity = event.Billing_City__c,
                                    BillingCountry = AS_AddressDataUtility.mapCountryCode(event.Billing_Country__c),
                                    BillingStreet = Event.Billing_Street__c,
                                    BillingPostalCode = event.Billing_Postal_Code__c,
                                    Business_Unit__c = event.Business_Unit__c,
                                    Distribution_Channel__c = event.Distribution_Channel__c,
                                    ERP_Account_Status__c = event.ERP_Account_Status__c,
                                    ERP_Customer_Group__c = event.ERP_Customer_Group__c,
                                    ERP_Level__c = event.ERP_Level__c,
                                    ERP_Number__c = event.ERP_Number__c,
                                    ERP_Number_ID__c = event.ERP_Number_ID__c,
                                    ERP_Search_2__c = event.ERP_Search_2__c,
                                    ERP_Search_3__c = event.ERP_Search_3__c,
                                    ERP_Search_Key__c = event.ERP_Search_Key__c,
                                    Segment__c = event.Segment__c,
                                    VAT_Id__c = event.VAT_Number__c,
                                    Web_Account_Type__c = event.Web_Account_Type__c,
                                    Parent = parentAccount
                                    );
                
                // add the account record to the list of Accounts to be upserted
                listAccounts.add(account);
                
            } 
            
            else {
                // Create or update the Account, without parent
                Account account = new Account(
                                    Name=event.Account_Name__c,
                                    Account_Owner_in_ERP__c = event.Account_Owner_in_ERP__c,
                                    BillingCity = event.Billing_City__c,
                                    BillingCountry = AS_AddressDataUtility.mapCountryCode(event.Billing_Country__c),
                                    BillingStreet = Event.Billing_Street__c,
                                    BillingPostalCode = event.Billing_Postal_Code__c,
                                    Business_Unit__c = event.Business_Unit__c,
                                    Distribution_Channel__c = event.Distribution_Channel__c,
                                    ERP_Account_Status__c = event.ERP_Account_Status__c,
                                    ERP_Customer_Group__c = event.ERP_Customer_Group__c,
                                    ERP_Level__c = event.ERP_Level__c,
                                    ERP_Number__c = event.ERP_Number__c,
                                    ERP_Number_ID__c = event.ERP_Number_ID__c,
                                    ERP_Search_2__c = event.ERP_Search_2__c,
                                    ERP_Search_3__c = event.ERP_Search_3__c,
                                    ERP_Search_Key__c = event.ERP_Search_Key__c,
                                    Segment__c = event.Segment__c,
                                    VAT_Id__c = event.VAT_Number__c,
                                    Web_Account_Type__c = event.Web_Account_Type__c
                                    );
                
                // add the account record to the list of Accounts to be upserted
                listAccounts.add(account);
                
            }
        
        }
        
        // Upsert all accounts in the list using the External Id as the record identifier. Do the DML upsert outside the for loop as best practise
        try {
            // Set External ID as the key
            Schema.SObjectField extID = Account.Fields.ERP_Number_ID__c;
            // Upsert the Account(s). AllOrNone set to false to allow partial success
            Database.UpsertResult[] sr = Database.upsert(listAccounts, extID, false);
            
            // Check for success and failures
            //for (Database.UpsertResult sr : srList) {
            for (Integer i=0;i<sr.size();i++){
                if (sr[i].isSuccess()) {
                    
                    // Create Platform Event Log record for success
                    Platform_Event_Log__c event = new Platform_Event_Log__c (
                                    Platform_Event__c='UPDATE ACCOUNT',
                                    Success__c=TRUE,
                                    Key__c=listAccounts[i].ERP_Number_ID__c,
                                    Event_Data__c='ACCOUNT NAME: '+listAccounts[i].Name
                                    );
                                    
                    // add the Event Log record to the list of Event Logs to be inserted
                    listLog.add(event);
                }
                else {
                    // Operation failed, so get all errors                
                    for(Database.Error err : sr[i].getErrors()) {
                        System.debug('error has occurred.' + err.getStatusCode() + ': ' + err.getMessage());                    
                        System.debug('fields that affected this error: ' + err.getFields());
                        
                        // Send a failure back to the Trigger and retry using EventBus.RetryableException. See https://developer.salesforce.com/docs/atlas.en-us.platform_events.meta/platform_events/platform_events_subscribe_apex_refire.htm
                        errorString = err.getStatusCode() + ': ' + err.getMessage();
                        
                        if(errorString.length()>255) {
                            errorString=errorString.substring(0,255);
                        }
                        
                        // Create Platform Event Log record for failure
                        Platform_Event_Log__c event = new Platform_Event_Log__c (
                                        Platform_Event__c='UPDATE ACCOUNT',
                                        Success__c=FALSE,
                                        Key__c=listAccounts[i].ERP_Number_ID__c,
                                        Event_Data__c='ACCOUNT NAME: '+listAccounts[i].Name,
                                        Error_Message__c=errorString
                                        );
                                        
                        // add the Event Log record to the list of Event Logs to be inserted
                        listLog.add(event);
                    }
                }
                
            }
            
            // Insert the Platform Event Logs
            Database.SaveResult[] res = Database.insert(listLog, false);
            
            
        } catch (DmlException e) {
            // Process exception here
            System.debug('ERROR: ' + e.getMessage() );
            errorString = e.getMessage();
            
        }
        
        // Return the result
        //return errorString;
        return '';
        
    }
    
}