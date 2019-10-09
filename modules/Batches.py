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
    all_batch_sql_file = 'C:\\Users\\robert.anderson\\PycharmProjects\\UrgentFeedback\\SQL\\' \
                         'Filings\\Warehouse Views\\V_FILINGS_BATCH_ACCOUNT_LIST.sql'
    dl_dir = r'S:\Folders\Filings\New batches to be filed - 2018'

    # dl_dir = r'C:\users\robert.anderson\downloads'

    # The S: drive in for the download directory is a result of downloading Citrix File's ShareFile Desktop App.
    # Once the file is saved to that directory, it is automatically uploaded to the ShareFile cloud by the app, which
    # removes the need to run a crawler through the ShareFile website.

    def __init__(self, console_output=False):
        self.console_output = console_output
        self.workbook = None
        self.sheet_1 = None
        self.sheet_2 = None
        self.user = ''
        self.password = ''
        self.account = ''
        self.warehouse = ''
        self.database = ''
        self.all_batch_sql = ''
        self.batch_types = ['New Account', 'Transfer Account']
        self.batch_number = 0
        self.date_auto = dt.date.today().strftime('%m.%d.%Y')
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
        end_date = dt.date.today()
        diff = end_date - start_date
        diff_weeks = diff.days / 7
        # self.batch_number = int(307)
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
        self.sheet_1 = self.workbook.create_sheet('New Accounts', 0)
        self.sheet_2 = self.workbook.create_sheet('Transfer Accounts', 1)

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
        self.sheet_1.append(col_names)
        self.sheet_2.append(col_names)
        for result in results:
            if result[len(result) - 2] == self.batch_number:
                if result[len(result)-1] == self.batch_types[0]:
                    self.sheet_1.append(result)
                elif result[len(result)-1] == self.batch_types[1]:
                    self.sheet_2.append(result)
        # cur.close()
        # con.close()
        if self.console_output:
            print('Saving the workspace to your downloads directory...')
        self.workbook.save(os.path.join(self.dl_dir, 'Batch {} {}.xlsx'.format(str(self.batch_number),
                                                                               self.date_auto)))
        self.workbook.close()
        if self.console_output:
            print('All files have been saved, and all connections have been closed.')

    def _stage_workbook(self):
        new_account_file_path = os.path.join(os.environ['userprofile'], 'downloads', 'new_account_upload_stage.csv')
        transfer_account_file_path = os.path.join(os.environ['userprofile'], 'downloads',
                                                  'transfer_account_upload_stage.csv')
        new_csv = pd.read_excel(
            os.path.join(self.dl_dir, 'Batch {} {}.xlsx'.format(str(self.batch_number), self.date_auto)),
            'New Accounts', index_col=None)
        transfer_csv = pd.read_excel(
            os.path.join(self.dl_dir, 'Batch {} {}.xlsx'.format(str(self.batch_number), self.date_auto)),
            'Transfer Accounts', index_col=None)
        new_csv.to_csv(new_account_file_path)
        transfer_csv.to_csv(transfer_account_file_path)
        # con = snowflake.connector.connect(
        #     user=self.user,
        #     password=self.password,
        #     account=self.account,
        #     warehouse=self.warehouse,
        #     database=self.database
        # )
        # if self.console_output:
        #     print('Asking Snowflake your question...')
        # cur = con.cursor()
        self.cur.execute("put file://? @%MY_UPLOADER_STAGE auto_compress=TRUE", new_account_file_path)
        self.cur.execute("put file://? @%MY_UPLOADER_STAGE auto_compress=TRUE", transfer_account_file_path)

    def _append_stage(self):
        transfer_upload_query_path = 'C:\\Users\\robert.anderson\\PycharmProjects\\UrgentFeedback\\SQL\\Filings' \
                                     '\\Transfer_Stage_Upload.sql'
        new_upload_query_path = 'C:\\Users\\robert.anderson\\PycharmProjects\\UrgentFeedback\\SQL\\Filings' \
                                '\\New_Stage_Upload.sql'
        transfer_query = open(transfer_upload_query_path, 'r')
        self.cur.execute(transfer_query.read())
        transfer_query.close()
        new_query = open(new_upload_query_path, 'r')
        self.cur.execute(new_query.read())
        new_query.close()

    def run_batch_handler(self):
        self._set_credentials()
        self._collect_queries()
        self._create_workbook()
        self._calc_batch()
        self._populate_workbook()
        # self._stage_workbook()
        # self._append_stage()

    def search_for_accounts(self, lst):
        """
        Pass a list of account numbers, as 7 digits, to have a dictionary returned of matching files in the default
        directory.
        :param lst: list of 7 digit accounts .
        :return: dictionary of accounts found in a file.
        """
        batch_regex = re.compile(r'Batch \d{3}', re.I)
        acct_regex = re.compile(r'\d{7}')
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
