import pandas as pd
import os

dl_dir = r'S:\Folders\Filings\New batches to be filed - 2018'
file_name = r'Batch 308 10.02.2019.xlsx'

xl_file = pd.ExcelFile(os.path.join(dl_dir, file_name)) # used to find the sheet names
xl_auto_df = pd.read_excel(os.path.join(dl_dir, file_name))
xl_auto_df2 = pd.read_excel(os.path.join(dl_dir, file_name), sheet_name=1)

xl_auto_df['BATCH_TYPE'] = xl_file.sheet_names[0]
xl_auto_df2['WEEK_BATCH'] = xl_auto_df['WEEK_BATCH'][0]
xl_auto_df2['BATCH_TYPE'] = xl_file.sheet_names[1]

xl_auto_df2.rename(columns={'INSTALLATION_DATE': 'INSTALL_DATE', 'PROJECT_NUMBER': 'PROJECT_NAME'}, inplace=True)

concat_df = pd.concat([xl_auto_df, xl_auto_df2], ignore_index=True)
concat_df.to_csv(os.path.join(os.environ['userprofile'], 'downloads', 'column_fix.csv'), index=False)

# todo: Make a program that will iterate over the current batch files, and collect meta data to verify that this
#   mehtod of conversion will work for all present files.
