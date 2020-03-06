# Batch Handling

## Process Steps
* civ: class instance variable
* cv: class variable
* mv: method variable
1. set credentials.
    1. load os environment variable to a json object.
    2. assign variable k,v pairs to civ* dictionary.
2. collect queries.
    1. Open file path stored in cv*.
    2. Assign file text to civ*.
3. create workbook.
    1. Assign a civ* with a Workbook object in the openpyxl module.
    2. Iterate through the civ* sheet names, and create a page in the workbook for each name.
4. calculate batch number.
    1. finds the difference, in weeks, from the batch end date, and the batch start date.
    2. assigns civ* with the difference, resulting in the batch number.
5. populate workbook.
    1. Assign snowflake connector object to civ*.
    2. Run the query, and assign the query results to a civ*.
    3. Generate a list of column names from the results.
    4. Assign all the query records to a mv*
    5. Iterate over the sheets in the workbook, and place the column names as the headers.
    6. Iterate over the query records, and write the record to the corresponding sheet, if it's the correct batch number.