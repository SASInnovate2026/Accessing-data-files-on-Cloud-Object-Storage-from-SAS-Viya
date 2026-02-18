/* Interactive User */
/* geldmui userid: geldmui@gelenable.sas.com */
/* geldmui pw: https://gelweb.race.sas.com/scripts/gelenable/users/geldmui@gelenable.sas.com.txt */
/* geldmui double authentication token if needed (MFA): https://geltotp.gelenable.sas.com/user/geldmui@gelenable.sas.com */
/* Direct access (read-only) to the ADLS Storage Account using Azure Portal: */
/* https://portal.azure.com/#@gelenable.sas.com/resource/subscriptions/5483d6c1-65f0-400d-9910-a7a448614167/resourceGroups/GEL_Storage_Accounts/providers/Microsoft.Storage/storageAccounts/geldmrepository/storagebrowser */

/************************** */
/* ADLS Connection – Part 1 */ 
/************************** */

   filename getinfo url "https://gelgitlab.race.sas.com/GEL/utilities/edge/-/raw/main/scripts/sas/collect_adls_info.sas" ;
   %include getinfo / nosource2 ;
   options azuretenantid="&TENANT_ID" ;

   %let requestbody = "client_id=&APP_ID.%str(&)scope=https://geldmrepository.blob.core.windows.net/.default offline_access" ;

   *** Request Device Code and User Code ***;
   filename devcode temp;
   proc http
     method="POST"
     url="https://login.microsoftonline.com/&TENANT_ID./oauth2/v2.0/devicecode"
     ct="application/x-www-form-urlencoded"
     in=&requestbody
     out=devcode;
   quit;

   *** Parse the response (JSON) ***;
   libname devcode json fileref=devcode;
   data _null_;
     set devcode.root;
     call symputx('device_code', device_code);
     call symputx('user_code', user_code);
     call symputx('interval', interval);
     put "*****************************************************";
     put "PLEASE LOG IN:";
     put "1. Go to: https://microsoft.com/devicelogin";
     put "2. Enter code: " user_code;
     put "*****************************************************";
   run;

               ***************************************************;
               *** Interactive User:                           ***;
               *** Cntl-Click the URL as instructed in the log ***;
               *** and enter the code you received.            ***;
               ***************************************************;


/************************** */
/* ADLS Connection – Part 2 */ 
/************************** */

   data _null_; call symputx('azure_cache_path', getoption('AZUREAUTHCACHELOC')); run;

   filename authcode "&azure_cache_path/.sasadls_1001.json";;

   *** Request the Device Code Authorization Token ***;
   %let requestbody2 = "grant_type=urn:ietf:params:oauth:grant-type:device_code%str(&)client_id=&APP_ID%str(&)device_code=&device_code" ;

   proc http
     method="POST"
     url="https://login.microsoftonline.com/&TENANT_ID./oauth2/v2.0/token"
     ct="application/x-www-form-urlencoded"
     in=&requestbody2
     out=authcode;
   quit;

   libname authcode json fileref=authcode;

   data _null_;
       retain success 0;
       set authcode.alldata end=eof;
       if p1 = "refresh_token" then success = 1;
       if eof then if success then do;
            put "********************************************************";
            put "Athentication Successful!!  Please move to the next step.";
            put "********************************************************";
         end;
         else do;
            put "*****************************************************";
            put "Please run ADLS Connection - Part 2 again.";
            put "*****************************************************";
         end;   
   run;


/******************* */
/* Deltalake on ADLS */ 
/******************* */

   *** Create a SAS library to the Azure Blob directory ***;
   libname duck_az duckdb file_path="az://data/yellow_taxi/deltalake" file_type=delta  azure_tenant_id="&TENANT_ID"
     azure_accountName="&AZ_STORAGE_ACCOUNT"
     azure_client_id="&CLIENT_ID"
     azure_client_secret="&CLIENT_SECRET" ;

   *** Validate the Library ***;
   proc datasets lib=duck_az ;  quit ;

   *** Run analytics directly on the Blob Deltalake file ***;
   proc anova data=duck_az.yellow_tripdata plots=none;
      class payment_type;
      model total_amount = payment_type;    
      title "One-Way ANOVA: Fare by Payment Type";
   quit;


/***************** */
/* Parquet on ADLS */
/***************** */

   *** Create a SAS library to the Azure Blob directory ***;
   libname duck_pq duckdb file_path="az://data/yellow_taxi/parquet" file_type=parquet
     azure_tenant_id="&TENANT_ID"
     azure_accountName="&AZ_STORAGE_ACCOUNT"
     azure_client_id="&CLIENT_ID"
     azure_client_secret="&CLIENT_SECRET" 
     directories_as_data=yes;

   *** Validate the Library ***;
   proc datasets lib=duck_pq ; quit ;

   *** Run analytics directly on the Blob Deltalake file ***;
   proc tabulate data=duck_pq.yellow_tripdata_all_partitioned;
     class RatecodeID;
     var tip_amount;
     table RatecodeID, tip_amount*Mean;
   run; 


/************************** */
/* CAS on Iceberg from ADLS */ 
/************************** */

   *** Create a SAS library to the Azure Blob directory ***;
   libname duck_ic duckdb file_path="az://data/yellow_taxi/iceberg/yellow_taxi_warehouse/yt" file_type=iceberg 
     azure_tenant_id="&TENANT_ID"
     azure_accountName="&AZ_STORAGE_ACCOUNT"
     azure_client_id="&CLIENT_ID"
     azure_client_secret="&CLIENT_SECRET" ;

   *** Validate the Library ***;
   proc datasets lib=duck_ic ; quit ;

   *** Load the Iceberg data to CAS ***;
   cas casauto sessopts=(metrics=true);

   proc casutil;
     load data=duck_ic.yellow_tripdata casout="tripdata" outcaslib="casuser" replace;
   quit;  

   *** Run CAS analytics on the loaded file ***;
   proc cas;
       simple.summary  /
          table={name="tripdata" caslib="casuser" groupBy="mnth1"
                 computedVars = {{name="mnth1"}}
                 computedVarsProgram = 'mnth1 = month(datepart(pickup_datetime));'}
          inputs={"fare_amount","tip_amount", "trip_distance"}
          subSet={"MEAN", "MIN", "MAX"};          
   run;

   cas casauto terminate;


/****************** */
/* SAS7BDAT on ADLS */
/****************** */

   *** Create a SAS library on the SAS Compute Server ***;
   libname clobj "/home/student/";

   *** Copy the dataset from blob storage to the local library ***;
   *** Parameters:  Dataset name, adls path, adls file system, target libname ***;
   %include "/home/student/Courses/CLOBJ/ReadSAS7BDATonAzureBlob.sas";
   %ReadSAS7BDATonAzureBlob(prdsal3,clobj,data,clobj);

   *** Process using SAS advanced file access capabilities ***; 
   data sample;
    start=ceil(20*ranuni(-1));
     drop start;
         do i=start to nobs by 20;
           set clobj.prdsal3 nobs=nobs point=i;
           output;
         end;
    stop;
   run;


/*************************** */
/* CAS on SAS7BDAT from ADLS */
/*************************** */

   *** Create a CAS session ***;
   cas casauto sessopts=(metrics=true);

   *** Copy the dataset from blob storage to the compute server ***;
   *** Parameters:  Dataset name, adls path, adls file system, target caslib ***;
   %include "/home/student/Courses/CLOBJ/LoadCASSAS7BDATonAzureBlob.sas";
   %LoadCASSAS7BDATonAzureBlob(prdsal3,clobj,data,casuser);

   *** Run CAS analytics on the loaded file ***; 
   proc cas;
    simple.summary /
        table={caslib="casuser", name="prdsal3",
               groupBy={"country", "state"} },
        inputs={"actual", "predict"},    
        subSet={"MEAN", "SUM", "MIN", "MAX", "N"};
   quit;