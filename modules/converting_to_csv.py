import os
import pandas as pd
from .snowflake import SnowflakeHandler
from . import Validation


class Converter:
    def __init__(self, debug_text=False, schema='', primary_table='', directory_to_convert=None, file_pattern=''):
        self.debug_text = debug_text
        self.project_dir = os.path.join(os.environ['userprofile'], 'PycharmProjects', 'Batch Handling')
        self.invoice_dir = os.path.join(self.project_dir, 'audit', 'invoices')
        self.convert_dir = directory_to_convert
        self.convert_csv_dir = directory_to_convert + '_csv'
        self.convert_types_dir = directory_to_convert + '_types'
        self.file_pattern = file_pattern
        self.total_df = pd.DataFrame()
        self.df_dir = {}
        self.df_list = []
        self.sn = SnowflakeHandler(schema=schema, primary_table=primary_table, console_output=debug_text)

    def _add_f_to_df(self, full_file):
        file_basename = os.path.basename(full_file)
        file_name = file_basename[:-5]  # remove the ".xlsx" from the end of the file name
        if os.path.exists(full_file):  # If the file is in the directory
            file_pages = pd.ExcelFile(full_file).sheet_names
            # If the file being evaluated is an old invoice
            if self.file_pattern == Validation.old_invoice_file_pattern.value:
                ct = len(file_pages)
                while ct > 0:
                    if ct != 0:
                        i = ct - 1  # place the index value
                        if Validation.old_invoice_original_re.value.match(file_pages[i]) is None \
                                and Validation.old_invoice_termination_re.value.match(file_pages[i]) is None:
                            file_pages.remove(file_pages[i])
                            ct -= 1
                        else:
                            if self.debug_text:
                                print(f'{file_pages[i]} is a wanted page.')
                            ct -= 1
            file_dfs = pd.read_excel(full_file, file_pages)  # Create a method level DataFrame of the file
            full_df = None
            for page_df in file_dfs:
                if full_df is None:
                    full_df = file_dfs[page_df]
                else:
                    full_df = full_df.append(file_dfs[page_df])

            full_df[Validation.Keys.FILE_NAME.value] = file_name

            # Check for similar DataFrame objects in the class list.
            if len(self.df_list) == 0:
                self.df_list.append(full_df)
            for df in self.df_list:  # iterate over the existing list of DataFrame objects
                if list(df.columns.values) == list(full_df.columns.values):  # if it matches
                    df = df.append(full_df)  # Append the two data sets together

            for i in range(len(self.df_list)):
                df = self.df_list[i]
                if list(df.columns.values) == list(full_df.columns.values):
                    self.df_list[i] = df.append(full_df)
                    break
                if i + 1 == len(self.df_list):
                    self.df_list.append(full_df)
            if os.path.exists(self.convert_csv_dir):  # Validate the csv directory
                full_df.to_csv(os.path.join(self.convert_csv_dir, file_name + '.csv'),
                               index=False)  # export the df to a csv in the csv directory with a new name
            else:
                os.mkdir(self.convert_csv_dir)  # Make the directory if it doesn't exist.
                full_df.to_csv(os.path.join(self.convert_csv_dir, file_name + '.csv'),
                               index=False)  # export the df to a csv in the csv directory with a new name
            self.df_dir[full_file] = full_df  # Add the method level DataFrame to the class level DataFrame dictionary
            self.total_df = self.total_df.append(full_df,
                                                 ignore_index=True,
                                                 sort=False)  # append the method DataFrame to the class DataFrame

    def convert(self):
        for f in os.listdir(self.convert_dir):  # Iterate over the list of files
            full_file = os.path.join(self.convert_dir, f)
            if self.debug_text:
                print(f'{f} is now being evaluated')
            self._add_f_to_df(full_file)  # pass the file to the DataFrame class method
        self.total_df.to_csv(os.path.join(self.project_dir, 'combined_detail_csv.csv'),
                             index=False)  # Save the class level DataFrame to the csv directory
        for df in self.df_dir:
            print(f'{df}: {len(self.df_dir[df].columns)}')  # Debug the details of the DataFrame dictionary
        print(f'{len(self.df_list)} unique DataFrame formats found.')
        type_count = len(self.df_list)
        for df in self.df_list:
            if os.path.exists(self.convert_types_dir):  # Make sure the types directory exists
                df.to_csv(os.path.join(self.convert_types_dir, f'File Type {type_count}.csv'), index=False)
                type_count -= 1
            else:
                os.mkdir(self.convert_types_dir)  # Create the types directory if it doesn't exist
                df.to_csv(os.path.join(self.convert_types_dir, f'File Type {type_count}.csv'), index=False)
                type_count -= 1

    def upload_to_snowflake(self, update_type):
        self.sn.run_table_updates(files_dir=self.convert_csv_dir,
                                  file_pattern=self.file_pattern,
                                  update_type=update_type)

    def merge_csv_files(self):
        """
        Turns a directory of .csv files into a single csv file, derived from
        the object's "directory_to_convert" parameter
        :return: None
        :rtype: None
        """
        total_df = None
        for f in os.listdir(self.convert_types_dir):
            f_df = pd.read_csv(os.path.join(self.convert_types_dir, f))
            if total_df is None:
                total_df = f_df
            else:
                total_df = total_df.append(f_df, sort=False)
        total_df.to_csv(os.path.join(self.project_dir, f'total df for {os.path.basename(self.convert_dir)}.csv'),
                        index=False)
