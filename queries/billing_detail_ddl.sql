BEGIN;

CREATE OR REPLACE TABLE D_POST_INSTALL.T_FILINGS_INVOICE_DETAILS
    COPY GRANTS
(
    STATE_COUNTY                  VARCHAR COMMENT 'Combination of state and county',
    STATE                         VARCHAR COMMENT 'State where the filings was submitted',
    COUNTY                        VARCHAR COMMENT 'County where the filings was submitted',
    BATCH_ID                      VARCHAR COMMENT 'Non-unique classification used by LD',
    FILING_TYPE                   VARCHAR COMMENT 'Non-unique classification used by LD',
    DATE_BATCH_RECEIVED_FROM_VSLR DATE COMMENT 'Date that the account was added to ShareFile',
    AR_NUMBER                     VARCHAR COMMENT 'Supposed to be the Service Name used in the T_SERVICE table.',
    PROJECT                       VARCHAR COMMENT 'Supposed to be the Service Name used in the T_SERVICE table.',
    FEE                           FLOAT COMMENT 'Charge from LD to VSLR'
) COMMENT = 'Table containing invoice details, provided by LD, for processing filing requests to the local Counties.';

DESCRIBE TABLE D_POST_INSTALL.T_FILINGS_INVOICE_DETAILS;
GRANT SELECT ON TABLE D_POST_INSTALL.T_FILINGS_INVOICE_DETAILS TO GENERAL_REPORTING_R;

ROLLBACK;
COMMIT;
