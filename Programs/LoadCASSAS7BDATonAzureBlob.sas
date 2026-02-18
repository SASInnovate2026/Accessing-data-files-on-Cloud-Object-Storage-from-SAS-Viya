*** SAS macro to load a SAS dataset on Azure Blob storage to CAS ***;

%macro LoadCASSAS7BDATonAzureBlob(dtstname,adlspath,adlsfs,cslib);

    *** Set the path of the target libname ***;
    %let lbpath = /tmp;

    *** Create the full path string to the source file ***;
    %let fullfilepath=&adlspath/&dtstname..sas7bdat ;

    *** Create a connection to the source file ***;
    filename _in adls "&fullfilepath"
       recfm=n
       applicationid="&APP_ID"
       accountname="&AZ_STORAGE_ACCOUNT"
       filesystem="&adlsfs";

   *** Create a connection to the target file ***;
   filename _out "&lbpath./&dtstname..sas7bdat" recfm=n lrecl=32767;

    *** Copy the file to the target location ***;
    data _null; rc = fcopy('_in', '_out'); if rc=0 then put "File Transferred!"; run ;

    libname lcs7bab "&lbpath";

    *** Load the copied file to CAS ***;
    proc casutil;
        droptable casdata="&dtstname" incaslib="&cslib" quiet; run;

        load data=lcs7bab.&dtstname casout="&dtstname"
        outcaslib="&cslib" promote; run;
    quit;  

    *** Clear the file and file definitions ***;
    data _null_; if (fexist('_out')) then rc=fdelete('_out'); run;
    libname lcs7bab clear;

%mend;
