BEGIN;
CREATE OR REPLACE TABLE D_POST_INSTALL.T_FILING_BATCH_UPLOAD_FILES
    COPY GRANTS
(
    SERVICE_NAME           VARCHAR COMMENT 'Service Name of the account as shown in Salesforce',
    PROJECT_NAME           VARCHAR COMMENT 'Project Name of the account as shown in Salesforce',
    CUSTOMER_1_FIRST       VARCHAR COMMENT 'First given name of the contract signer',
    CUSTOMER_1_MIDDLE      VARCHAR COMMENT 'Middle given name of the contract signer',
    CUSTOMER_1_SUFFIX      VARCHAR COMMENT 'Contract signer Suffix, if applicable',
    CUSTOMER_1_LAST        VARCHAR COMMENT 'Last given name of the contract signer',
    CUSTOMER_2_FIRST       VARCHAR COMMENT 'First given name of the co-signer',
    CUSTOMER_2_MIDDLE      VARCHAR COMMENT 'Middle given name of the co-signer',
    CUSTOMER_2_SUFFIX_NAME VARCHAR COMMENT 'Co-signer suffix, if applicable',
    CUSTOMER_2_LAST_NAME   VARCHAR COMMENT 'Last given name of the co-signer',
    SERVICE_ADDRESS        VARCHAR COMMENT 'Street Address of the installed system',
    SERVICE_CITY           VARCHAR COMMENT 'City Address of the installed system',
    SERVICE_COUNTY         VARCHAR COMMENT 'County Address of the installed system',
    SERVICE_STATE          VARCHAR COMMENT 'State Address of the installed system',
    SERVICE_ZIP_CODE       VARCHAR COMMENT 'Zip Code Address of the installed system',
    INSTALL_DATE           VARCHAR COMMENT 'Date that the PV System was installed on the home',
    TRANSACTION_DATE       VARCHAR COMMENT 'Date that the contract signer signed the contract',
    TERMINATION_DATE       VARCHAR COMMENT '20 years after the In-service (Installation/Electrical Service Chage) date',
    CONTRACT_TYPE          VARCHAR COMMENT 'Type of contract signed by the contract signer',
    WEEK_BATCH             VARCHAR COMMENT 'Batch number that the account was listed on',
    BATCH_TYPE             VARCHAR COMMENT 'Which type of request is being sent for processing',
    FILE_NAME              VARCHAR COMMENT 'Name of the batch file sent for processing'
) COMMENT = 'This table contains all of the records compiled into batches that are sent for county filings to be processed'
;

GRANT SELECT ON TABLE D_POST_INSTALL.T_FILING_BATCH_UPLOAD_FILES TO GENERAL_REPORTING_R;
SELECT * FROM D_POST_INSTALL.T_FILING_BATCH_UPLOAD_FILES;

COMMIT;
ROLLBACK;
