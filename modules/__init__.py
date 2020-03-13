from enum import Enum
import os
import re
import json

conda_env_name = os.environ['conda_default_env'] if 'conda_default_env' in os.environ else 'na'
default_dl_dir = os.path.join(os.environ['userprofile'], 'Downloads')
project_dir = os.path.join(os.environ['userprofile'], 'PycharmProjects', 'Batch Handling')
temp_batch_dir = os.path.join(project_dir, 'temp')


class Validation(Enum):
    class Keys(Enum):
        BATCH = 'Batch'
        CONTINUATION = 'Continuation'
        INVOICE = 'Invoice'
        WORKBOOK = 'wb'
        WORKSHEET = 'sheets'
        FILE_NAME = 'File Name'

    batch_save_location_local = 'Local'
    batch_save_location_sharefile = 'Sharefile'

    _batch_primary_table = 'T_FILING_BATCH_UPLOAD_FILES'
    _batch_file_pattern = 'Batch'  # Used in the Snowflake COPY INTO command
    _batch_file_re = re.compile(r'Batch.*\d*.*', re.I)

    _continuation_primary_table = 'T_FILING_BATCH_UPLOAD_FILES'
    _continuation_file_pattern = 'Continuation'  # Used in the Snowflake COPY INTO command
    _continuation_file_re = re.compile(r'Continuation.*\d*.*', re.I)

    _invoice_primary_table = 'T_FILINGS_INVOICE_DETAILS'
    _invoice_file_pattern = 'Invoice'  # Used in the Snowflake COPY INTO command
    _invoice_file_re = re.compile(r'Invoice.*\d*.*Detail.*', re.I)

    old_invoice_file_pattern = 'Batch*Fund*'
    old_invoice_original_re = re.compile(r'.*Origin.*')
    old_invoice_termination_re = re.compile(r'.*Termi.*')
    old_invoice_issues_re = re.compile(r'.*Issue.*')

    update_type_append = 'append'
    update_type_replace = 'replace'

    missing_file_type_batch = Keys.BATCH.value
    missing_file_type_invoice = Keys.INVOICE.value
    missing_file_type_continuation = Keys.CONTINUATION.value

    weekly_batch_structure = {
        Keys.BATCH.value: ['New Account', 'Transfer Account', 'Refinance Account'],
        Keys.CONTINUATION.value: ['Continuation'],
    }
    dl_dir_match = {
        batch_save_location_local: {
            Keys.BATCH.value: default_dl_dir,
            Keys.CONTINUATION.value: default_dl_dir,
            Keys.INVOICE.value: default_dl_dir
        },
        batch_save_location_sharefile: {
            Keys.BATCH.value: r'S:\Folders\Filings\New batches to be filed - 2018',
            Keys.CONTINUATION.value: r'S:\Folders\Filings\Continuations',
            Keys.INVOICE.value: r'S:\Folders\Filings\Funding requests and invoices\Funding Request'
        }
    }

    file_type_search_dir = {
        missing_file_type_batch: dl_dir_match[batch_save_location_sharefile][Keys.BATCH.value],
        missing_file_type_invoice: dl_dir_match[batch_save_location_sharefile][Keys.INVOICE.value],
        missing_file_type_continuation: dl_dir_match[batch_save_location_sharefile][Keys.CONTINUATION.value]
    }
    file_type_re = {
        missing_file_type_batch: _batch_file_re,
        missing_file_type_invoice: _invoice_file_re,
        missing_file_type_continuation: _continuation_file_re
    }
    file_type_primary_tables = {
        missing_file_type_batch: _batch_primary_table,
        missing_file_type_invoice: _invoice_primary_table,
        missing_file_type_continuation: _continuation_primary_table
    }
    file_type_file_patterns = {
        missing_file_type_batch: _batch_file_pattern,
        missing_file_type_invoice: _invoice_file_pattern,
        missing_file_type_continuation: _continuation_file_pattern
    }


class Messages(Enum):
    no_cur = 'self.cur evaluated to False'
    wrong_update_type = f'Sorry, unsupported update type. Please choose "{Validation.update_type_append}" or ' \
                        f'"{Validation.update_type_replace}"'
    main_error = 'Sorry, please pass two lists of equal length to the "function_list" and "args_list" parameters'
