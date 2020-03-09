from . import os
from . import json
from . import Messages
from . import default_dl_dir
import snowflake.connector
from pathlib import Path


class SnowflakeHandler:
    """
    Prerequisites:
        1) You need to create a JSON formatted Environment Variable, named "SNOWFLAKE_KEY", with the following keys.
            1) USER
            2) PASSWORD
            3) ACCOUNT
            4) WAREHOUSE
            5) DATABASE
        Please see Snowflake documentation for the definitions of the required fields.
    """

    dl_dir = default_dl_dir

    def __init__(self, console_output=False, schema=None, primary_table=''):
        self.console_output = console_output
        self.primary_table = primary_table
        self.snowflake_credentials = {}
        self.temp_query = ''
        self.schema = schema
        self.con = False
        self.cur = False
        self.truncate_table_text = """TRUNCATE TABLE d_post_install.<<tn>>;"""
        self.populate_table_text = """COPY INTO D_POST_INSTALL.<<tn>>
FROM @MY_UPLOADER_STAGE
FILE_FORMAT = (format_name = 'CX_CSV_WITH_HEADER')
PATTERN = '.*<<pat>>.*'
ON_ERROR = 'skip_file';"""
        self._set_credentials()

    def _set_credentials(self):
        if self.console_output:
            print('Collecting Snowflake credentials from system environment...')
        snowflake_json = json.loads(os.environ['SNOWFLAKE_KEY'])
        for k, v in snowflake_json.items():
            self.snowflake_credentials[k.lower()] = v
        if self.console_output:
            print('credentials have been collected and assigned')

    def set_con_and_cur(self):
        self.con = snowflake.connector.connect(  # Create the snowflake connection in the class instance variable
            **self.snowflake_credentials  # Use the credentials found in the environment variable
        )
        self.cur = self.con.cursor()

    def close_con_and_cur(self):
        self.cur.close()
        self.con.close()
        self.cur = False
        self.con = False

    def run_query_file(self, file_path):
        """
        :param file_path: the path to the sql file that needs to be executed
        :return: A list of tuples containing the query result data.
        """
        if self.cur:
            with open(file_path, 'r') as f:
                query_string = f.read()
            results = self.cur.execute(query_string)
            data = results.fetchall()
            return data
        else:
            return Messages.no_cur.value

    def run_query_string(self, query_string):
        """
        :param query_string: a string of sql
        :return: A list of tuples containing the query result data.
        """
        if self.cur:
            results = self.cur.execute(query_string)
            data = results.fetchall()
            return data
        else:
            return Messages.no_cur.value

    def sql_command(self, command_string):
        self.set_con_and_cur()
        if self.cur:
            response = self.cur.execute(command_string)
            self.con.commit()
        else:
            return Messages.no_cur.value
        self.close_con_and_cur()

    def _truncate_table(self):
        command = self.truncate_table_text.replace('<<tn>>', self.primary_table)
        if self.console_output:
            print(command)
        self.sql_command(command)

    def _stage_file(self, files_dir):
        self.sql_command('remove @my_uploader_stage')
        for f in os.listdir(files_dir):
            full_file = Path(os.path.join(files_dir, f))
            if self.console_output:
                print(full_file)
            stage_file_text = f"""put 'file://C:/{'/'.join(full_file.parts[1:])}' @my_uploader_stage auto_compress = true;"""
            if self.console_output:
                print(stage_file_text)
            self.sql_command(stage_file_text)
        self.sql_command('remove @my_uploader_stage')

    def _populate_table(self, file_pattern):
        command = self.populate_table_text.replace('<<tn>>', self.primary_table)
        command = command.replace('<<pat>>', file_pattern)
        if self.console_output:
            print(command)
        self.sql_command(command)

    def run_table_updates(self, files_dir, file_pattern, update_type):
        if update_type == 'append':
            self._stage_file(files_dir)
            self._populate_table(file_pattern)
        elif update_type == 'replace':
            self._truncate_table()
            self._stage_file(files_dir)
            self._populate_table(file_pattern)
        else:
            return Messages.wrong_update_type.value
