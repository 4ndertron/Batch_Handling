import snowflake.connector
import os
import re
import json
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
            1) USERNAME
            2) PASSWORD
            3) ACCOUNT
            4) WAREHOUSE
            5) DATABASE
        Please see Snowflake documentation for the definitions of the required fields.
    """
    all_batch_sql_file = os.path.join(os.environ['userprofile'],
                                      'PycharmProjects',
                                      'Batch Handling',
                                      'queries',
                                      'account_list.sql')
    dl_dir = r'S:\Folders\Filings\New batches to be filed - 2018'

    # dl_dir = r'C:\users\robert.anderson\downloads'

    # The S: drive in for the download directory is a result of downloading Citrix File's ShareFile Desktop App.
    # Once the file is saved to that directory, it is automatically uploaded to the ShareFile cloud by the app, which
    # removes the need to run a crawler through the ShareFile website.

    def __init__(self, console_output=False):
        self.console_output = console_output
        self.workbook = None
        self.sheets = []
        # todo: convert these hard-coded attributes into a dictionary object.
        self.user = ''
        self.password = ''
        self.account = ''
        self.warehouse = ''
        self.database = ''
        # End hard-coded attributes
        # todo: make a file that contains the distinct batch types from the query,
        #   instead of hard-coding the values here.
        self.batch_types = ['New Account', 'Transfer Account', 'Refinance Account']
        self.all_batch_sql = ''
        self.batch_number = 0
        self.date_auto = dt.date.today()
        # self.date_auto = dt.date(2020, 2, 19)
        self.date_manual = '09.25.2019'
        self.con = None
        self.cur = None

    def _set_credentials(self):
        if self.console_output:
            print('Collecting Snowflake credentials form system environment...')
        snowflake_json = json.loads(os.environ['SNOWFLAKE_KEY'])
        self.user = snowflake_json['USERNAME']
        self.password = snowflake_json['PASSWORD']
        self.account = snowflake_json['ACCOUNT']
        self.warehouse = snowflake_json['WAREHOUSE']
        self.database = snowflake_json['DATABASE']
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
            print('Finding how many times this file has been created...')
        start_date = dt.date(2013, 11, 2)
        end_date = self.date_auto
        diff = end_date - start_date
        diff_weeks = diff.days / 7
        # self.batch_number = int(328)
        self.batch_number = int(diff_weeks)
        if self.console_output:
            print('I know how many times this file has been created.')

    def _create_workbook(self):
        """
        This function is designed to save memory space until it is needed to generate the actual workbook.
        :return:
        """
        if self.console_output:
            print('Making a temporary workspace in memory')
        self.workbook = xl.Workbook('temp_batch.xlsx')
        for i in range(len(self.batch_types)):
            self.sheets.append(self.workbook.create_sheet(self.batch_types[i], i))

    def _populate_workbook(self):
        if self.console_output:
            print('Knocking on Snowflake\'s door...')
        self.con = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database
        )
        if self.console_output:
            print('Asking Snowflake your question...')
        self.cur = self.con.cursor().execute(self.all_batch_sql)
        col_names = [x[0] for x in self.cur.description]
        if self.console_output:
            print('Collecting results...')
        results = self.cur.fetchall()
        if self.console_output:
            print('Writing results to the temporary workspace...')
        for sheet in self.sheets:
            sheet.append(col_names)
        for result in results:
            if result[len(result) - 2] == self.batch_number:
                self.sheets[self.batch_types.index(result[len(result) - 1])].append(result)
        if self.console_output:
            print('Saving the workspace to your downloads directory...')
        self.workbook.save(os.path.join(self.dl_dir, 'Batch {} {}.xlsx'.format(str(self.batch_number),
                                                                               self.date_auto.strftime('%m.%d.%Y'))))
        self.workbook.close()
        if self.console_output:
            print('All files have been saved, and all connections have been closed.')

    def run_batch_handler(self):
        self._set_credentials()
        self._collect_queries()
        self._create_workbook()
        self._calc_batch()
        self._populate_workbook()

    def search_for_accounts(self, lst):
        """
        Pass a list of account numbers, as 6 or more digits, to have a dictionary returned of matching
        files in the default directory.
        :param lst: list of 6 or more digit accounts .
        :return: dictionary of accounts found in a file.
        """
        batch_regex = re.compile(r'Batch \d{3}', re.I)
        acct_regex = re.compile(r'\d{6,}')
        acct_list = {}
        for file in os.listdir(self.dl_dir):
            if self.console_output:
                print('checking {}'.format(file))
            mo = batch_regex.search(file)
            if mo:
                if self.console_output:
                    print('opening {}'.format(file))
                f = xl.load_workbook(os.path.join(self.dl_dir, file))
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
