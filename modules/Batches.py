from . import os
from . import re
from . import json
from . import Validation
from . import project_dir
import snowflake.connector
import openpyxl as xl
import datetime as dt
import pandas as pd


class BatchHandler:
    """
    This class is setup to do the following tasks:
        1) Create an excel workbook populated by two SQL queries.
        2) Run a headless chromedriver to upload the excel workbook to ShareFile.

    Prerequisites:
        1) You need to create a JSON formatted Environment Variable, named "SNOWFLAKE_KEY", with the following keys.
            1) USER
            2) PASSWORD
            3) ACCOUNT
            4) WAREHOUSE
            5) DATABASE
        Please see Snowflake documentation for the definitions of the required fields.
    """
    all_batch_sql_file = os.path.join(project_dir,
                                      'queries',
                                      'account_list.sql')

    def __init__(self, batch_end_date, save_location, console_output=False):
        self.console_output = console_output
        self.workbooks = {}
        self.sheets = []
        self.snowflake_credentials = {}
        self.all_batch_sql = ''
        self.batch_number = 0
        self.save_location = save_location
        self.batch_end_date = batch_end_date
        self.con = None
        self.cur = None
        self.file_structure = Validation.weekly_batch_structure.value

    def _set_credentials(self):
        if self.console_output:
            print('Collecting Snowflake credentials form system environment...')
        snowflake_json = json.loads(os.environ['SNOWFLAKE_KEY'])
        for k, v in snowflake_json.items():
            self.snowflake_credentials[k.lower()] = v
        if self.console_output:
            print('credentials have been collected and assigned')

    def _collect_queries(self):
        if self.console_output:
            print('Reading the queries...')
        open_sql = open(self.all_batch_sql_file, 'r')
        self.all_batch_sql = open_sql.read()
        open_sql.close()
        if self.console_output:
            print('I know what to ask Snowflake.')

    def _calc_batch(self):
        """
        This function will calculate the number of weeks passed since the start of the batch process.
        The batch process began on 2013-11-02
        :return: assigns self.batch_number the calculated batch number.
        """
        if self.console_output:
            print('Finding the batch number based on the batch date value passed during object creation...')
        start_date = dt.date(2013, 11, 2)
        diff = self.batch_end_date - start_date
        diff_weeks = diff.days / 7
        self.batch_number = int(diff_weeks)
        if self.console_output:
            print(f'Batch {self.batch_number} is being evaluated.')

    def _generate_workbooks(self):
        if self.console_output:
            print('Making a temporary workspace in memory')
        for file in self.file_structure:
            self.workbooks[file] = {}
            self.workbooks[file][Validation.workbook_key.value] = xl.Workbook(file + '.xlsx')
            self.workbooks[file][Validation.worksheet_key.value] = []
            for i in range(len(self.file_structure[file])):
                self.workbooks[file][Validation.worksheet_key.value].append(
                    self.workbooks[file][Validation.workbook_key.value].create_sheet(self.file_structure[file][i], i))

    def _populate_workbook(self):
        if self.console_output:
            print('Knocking on Snowflake\'s door...')
        self.con = snowflake.connector.connect(  # Create the snowflake connection in the class instance variable
            **self.snowflake_credentials  # Use the credentials found in the environment variable
        )
        if self.console_output:
            print('Asking Snowflake your question...')
        self.cur = self.con.cursor().execute(self.all_batch_sql)  # Retrieve the query results
        col_names = [x[0] for x in self.cur.description]  # Generate a list of column names
        if self.console_output:
            print('Collecting results...')
        results = self.cur.fetchall()  # Assign all the query records to a method variable
        if self.console_output:
            print('Writing results to the temporary workspace...')
        for wb in self.workbooks:
            for ws in self.workbooks[wb][Validation.worksheet_key.value]:
                ws.append(col_names)
        # for sheet in self.sheets:
        #     sheet.append(col_names)
        for result in results:  # Iterate through the results
            batch_index = len(result) - 2  # Get the index integer of the batch number column
            result_batch = result[batch_index]  # Get the batch value of the specific result
            if result_batch == self.batch_number:
                type_index = len(result) - 1  # Get the index integer of the batch type column
                result_type = result[type_index]  # Get the batch type value of the specific result
                for wb in self.workbooks:  # Iterate through the workbooks
                    if result_type in self.workbooks[wb][Validation.workbook_key.value].sheetnames:
                        self.workbooks[wb][Validation.worksheet_key.value][
                            self.workbooks[wb][Validation.workbook_key.value].sheetnames.index(result_type)].append(
                            result)  # Append the record to the sheet
        if self.console_output:
            print('Saving the workspaces to your designated location...')
        for wb in self.workbooks:
            dl_dir = Validation.dl_dir_match.value[self.save_location][wb]
            file_name = wb + f' {self.batch_number} {self.batch_end_date.strftime("%m.%d.%Y")}.xlsx'
            self.workbooks[wb][Validation.workbook_key.value].save(os.path.join(dl_dir, file_name))
            self.workbooks[wb][Validation.workbook_key.value].close()
        if self.console_output:
            print('All files have been saved, and all connections have been closed.')

    def run_batch_handler(self):
        self._set_credentials()
        self._collect_queries()
        self._generate_workbooks()
        self._calc_batch()
        self._populate_workbook()

    def search_for_accounts(self, lst):
        """
        Pass a list of account numbers, as 6 or more digits, to have a dictionary returned of matching
        files in the default directory.
        NOTE: This method be obsolete since the batch files have been added to the data warehouse
        :param lst: list of 6 or more digit accounts .
        :return: dictionary of accounts found in a file.
        """
        batch_regex = re.compile(r'Batch \d{3}', re.I)
        acct_regex = re.compile(r'\d{6,}')
        acct_list = {}
        for file in os.listdir(self.file_save_dir):
            if self.console_output:
                print('checking {}'.format(file))
            mo = batch_regex.search(file)
            if mo:
                if self.console_output:
                    print('opening {}'.format(file))
                f = xl.load_workbook(os.path.join(self.file_save_dir, file))
                fs = f.active
                if self.console_output:
                    print('iterating through the first two columns...')
                for row in fs.iter_cols(min_col=1, max_col=2, min_row=2, max_row=fs.max_row):
                    rmo = acct_regex.search(str(row))
                    if rmo:
                        if int(rmo.string) in lst:
                            acct_list[rmo.string] = file
            else:
                if self.console_output:
                    print('{} did not match the batch regex'.format(file))
        if self.console_output:
            print('returning the dictionary and ending the function call.')
        return acct_list
