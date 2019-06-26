import snowflake.connector
import os
import json
import openpyxl as xl
import datetime as dt


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
    new_account_sql_file = 'C:\\Users\\robert.anderson\\PycharmProjects\\' \
                           'UrgentFeedback\\SQL\\Filings\\FilingsNewAccounts.sql'
    transfer_accounts_sql_file = 'C:\\Users\\robert.anderson\\PycharmProjects\\' \
                                 'UrgentFeedback\\SQL\\Filings\\SepTransferCases.sql'
    dl_dir = r'S:\Folders\Filings\New batches to be filed - 2018'

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
        self.new_account_sql = ''
        self.transfer_accounts_sql = ''
        self.batch_number = 0

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
        open_sql = open(self.new_account_sql_file, 'r')
        self.new_account_sql = open_sql.read()
        open_sql.close()
        open_sql = open(self.transfer_accounts_sql_file, 'r')
        self.transfer_accounts_sql = open_sql.read()
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
        con = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database
        )
        if self.console_output:
            print('Asking Snowflake your question...')
        cur = con.cursor().execute(self.new_account_sql)
        col_names = [x[0] for x in cur.description]
        if self.console_output:
            print('Collecting results...')
        results = cur.fetchall()
        if self.console_output:
            print('Writing results to the temporary workspace...')
        self.sheet_1.append(col_names)
        for result in results:
            if result[len(result) - 1] == self.batch_number:
                self.sheet_1.append(result)
        cur = con.cursor().execute(self.transfer_accounts_sql)
        results = cur.fetchall()
        col_names = [x[0] for x in cur.description]
        self.sheet_2.append(col_names)
        for result in results:
            self.sheet_2.append(result)
        cur.close()
        con.close()
        if self.console_output:
            print('Saving the workspace to your downloads directory...')
        self.workbook.save(os.path.join(self.dl_dir, 'Batch {} {}.xlsx'.format(str(self.batch_number),
                                                                               dt.date.today().strftime(
                                                                                   '%m.%d.%Y'))))
        self.workbook.close()
        if self.console_output:
            print('All files have been saved, and all connections have been closed.')
            print('Now all you need to do is create a crawler to give the file to LD for you.')

    def run_batch_handler(self):
        self._set_credentials()
        self._collect_queries()
        self._create_workbook()
        self._calc_batch()
        self._populate_workbook()


if __name__ == '__main__':
    new_handle = BatchHandler()
    new_handle.run_batch_handler()
