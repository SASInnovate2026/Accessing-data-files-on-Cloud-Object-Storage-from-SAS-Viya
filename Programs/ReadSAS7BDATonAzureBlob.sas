*** SAS macro to transfer a SAS dataset from Azure Blob storage to a local SAS library ***;

%macro ReadSAS7BDATonAzureBlob(dtstname,adlspath,adlsfs,lbname);

    *** Extract the path of the target libname ***;
    %let lbpath = %sysfunc(pathname(&lbname));

    *** Create the full path string to the source file ***;
    %let fullfilepath=&adlspath/&dtstname..sas7bdat ;

    *** Create a connection to the source file ***;
    filename _in adls "&fullfilepath" recfm=n applicationid="&APP_ID"
        accountname="&AZ_STORAGE_ACCOUNT" filesystem="&adlsfs";

    *** Create a connection to the target file ***;
    filename _out "&lbpath./&dtstname..sas7bdat" recfm=n lrecl=32767;

    *** Copy the file to the target location ***;
    data _null; rc = fcopy('_in', '_out'); if rc=0 then put "File Transferred!"; run ;

%mend;
