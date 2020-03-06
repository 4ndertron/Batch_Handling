from enum import Enum
import os
import re
import json

conda_env_name = os.environ['conda_default_env']
default_dl_dir = os.path.join(os.environ['userprofile'], 'Downloads')
project_dir = os.path.join(os.environ['userprofile'], 'PycharmProjects', 'Batch Handling')


class Messages(Enum):
    no_cur = 'self.cur evaluated to False'
    wrong_update_type = 'Sorry, unsupported update type. Please choose "append" or "replace"'


class Validation(Enum):
    _batch_key = 'Batch'
    _continuations_key = 'Continuations'
    batch_save_location_local = 'Local'
    batch_save_location_sharefile = 'Sarefile'
    batch_primary_table = 'T_FILING_BATCH_UPLOAD_FILES'
    batch_file_pattern = 'Batch*.csv'
    invoice_primary_table = 'T_FILINGS_INVOICE_DETAILS'
    invoice_file_pattern = 'Invoice*Details.csv'
    old_invoice_file_pattern = 'Batch*Fund*'
    old_invoice_original_re = re.compile('.*Origin.*')
    old_invoice_termination_re = re.compile('.*Termi.*')
    old_invoice_issues_re = re.compile('.*Issue.*')
    update_type_append = 'append'
    update_type_replace = 'replace'
    main_error = 'Sorry, please pass two lists of equal length to the "function_list" and "args_list" parameters'

    workbook_key = 'wb'
    worksheet_key = 'sheets'

    weekly_batch_structure = {
        _batch_key: ['New Account', 'Transfer Account', 'Refinance Account'],
        _continuations_key: ['Continuation'],
    }
    dl_dir_match = {
        batch_save_location_local: {
            _batch_key: default_dl_dir,
            _continuations_key: default_dl_dir
        },
        batch_save_location_sharefile: {
            _batch_key: r'S:\Folders\Filings\New batches to be filed - 2018',
            _continuations_key: r'S:\Folders\Filings\Continuations'
        }
    }
