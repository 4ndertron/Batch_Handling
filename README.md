This is the second version of the entire Batch Handling process involving VSLR and LD.\
The process steps are:
1. Run Query
2. Save Query to Excel File
3. Upload File to ShareFile

Prerequisite steps to run the program include the following:
1. Add the query file-paths to variables found in "Batches.py"
2. Add the ShareFile App directory you want the file saved to a variable found in "Batches.py".
3. Make a JSON formatted Environment Variable, named "SNOWFLAKE_KEY" that includes these fields:
    1. USERNAME
    2. PASSWORD
    3. ACCOUNT
    4. WAREHOUSE
    5. DATABASE
    
Please see the official Snowflake Documentation for definitions of the required fields.

## Source
- [Ready to Process Ones](https://vivintsolar.lightning.force.com/lightning/r/Report/00O1M000007qnXGUAY/view?queryScope=userFolders)
- [Closed Ones](https://vivintsolar.lightning.force.com/lightning/r/Report/00O1M000007rBEcUAM/view?queryScope=userFolders)
