from enum import Enum
import os
import re
import json

conda_env_name = os.environ['conda_default_env']
default_dl_dir = os.path.join(os.environ['userprofile'], 'Downloads')
project_dir = os.path.join(os.environ['userprofile'], 'PycharmProjects', 'Batch Handling')
temp_batch_dir = os.path.join(project_dir, 'temp')


class Validation(Enum):
    class Keys(Enum):
        BATCH = 'Batch'
        CONTINUATIONS = 'Continuation'
        WORKBOOK = 'wb'
        WORKSHEET = 'sheets'
        FILE_NAME = 'File Name'

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

    weekly_batch_structure = {
        Keys.BATCH.value: ['New Account', 'Transfer Account', 'Refinance Account'],
        Keys.CONTINUATIONS.value: ['Continuation'],
    }
    dl_dir_match = {
        batch_save_location_local: {
            Keys.BATCH.value: default_dl_dir,
            Keys.CONTINUATIONS.value: default_dl_dir
        },
        batch_save_location_sharefile: {
            Keys.BATCH.value: r'S:\Folders\Filings\New batches to be filed - 2018',
            Keys.CONTINUATIONS.value: r'S:\Folders\Filings\Continuations'
        }
    }


class Messages(Enum):
    no_cur = 'self.cur evaluated to False'
    wrong_update_type = f'Sorry, unsupported update type. Please choose "{Validation.update_type_append}" or ' \
                        f'"{Validation.update_type_replace}"'
    main_error = 'Sorry, please pass two lists of equal length to the "function_list" and "args_list" parameters'
