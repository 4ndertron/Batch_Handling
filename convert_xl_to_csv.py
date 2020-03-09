import os
from modules.converting_to_csv import Converter
from modules import Validation
from modules import Messages


def update_invoice_details(*args):
    primary_table = Validation.invoice_primary_table.value
    file_pattern_text = Validation.invoice_file_pattern.value
    convert_directory = os.path.join(os.getcwd(), 'audit', 'invoices', 'details')
    converter = Converter(debug_text=args[0],
                          schema=args[1],
                          directory_to_convert=convert_directory,
                          file_pattern=file_pattern_text,
                          primary_table=primary_table)

    # %% Optional function block. Comment and un-comment the steps you want to do
    # converter.convert()
    # converter.upload_to_snowflake(update_type=args[2])
    # converter.merge_csv_files()


def update_old_invoice_details(*args):
    primary_table = Validation.invoice_primary_table.value
    file_pattern_text = Validation.old_invoice_file_pattern.value
    convert_directory = os.path.join(os.getcwd(), 'audit', 'invoices', 'old_invoice_details')
    converter = Converter(debug_text=args[0],
                          schema=args[1],
                          directory_to_convert=convert_directory,
                          file_pattern=file_pattern_text,
                          primary_table=primary_table)

    # %% Optional function block. Comment and un-comment the steps you want to do
    # converter.convert()
    # converter.upload_to_snowflake(update_type=args[2])
    converter.merge_csv_files()


def update_batch_upload_files(*args):
    primary_table = Validation.batch_primary_table.value
    file_pattern_text = Validation.batch_file_pattern.value
    convert_directory = os.path.join(os.getcwd(), 'audit', 'invoices', 'uploads')
    converter = Converter(debug_text=args[0],
                          schema=args[1],
                          file_pattern=file_pattern_text,
                          directory_to_convert=convert_directory,
                          primary_table=primary_table)

    # %% Optional function block. Comment and un-comment the steps you want to do
    # converter.convert()
    # converter.upload_to_snowflake(update_type=args[2])
    # converter.merge_csv_files()


def update_new_batch_upload_files(*args):
    passed_args = args
    print(f'Debug args in fun: {passed_args}')
    primary_table = Validation.batch_primary_table.value
    file_pattern_text = Validation.batch_file_pattern.value
    convert_directory = os.path.join(os.getcwd(), 'audit', 'invoices', 'new_uploads')
    converter = Converter(debug_text=args[0],
                          schema=args[1],
                          file_pattern=file_pattern_text,
                          directory_to_convert=convert_directory,
                          primary_table=primary_table)

    # %% Optional function block. Comment and un-comment the steps you want to do
    # converter.convert()
    # converter.upload_to_snowflake(update_type=args[2])
    # converter.merge_csv_files()


def main(function_list, args_list):
    if type(function_list).__name__ == 'list' \
            and type(args_list).__name__ == 'list' \
            and len(function_list) == len(args_list):
        for i in range(len(function_list)):
            print(f'Debug fun: {function_list[i].__name__}\nDebug args: {args_list[i]}')
            function_list[i](*args_list[i])
    else:
        if __name__ == '__main__':
            return Messages.main_error.value


if __name__ == '__main__':
    debug_text = True
    schema = 'D_POST_INSTALL'
    fun_list = []
    fun_args = []

    # %% begin function main parameter definitions
    primary_table_update_type = Validation.update_type_append.value  # Change if needed
    fun_list.append(update_old_invoice_details)  # Point to the function that correlates to the update type above
    fun_args.append((debug_text, schema, primary_table_update_type))
    # %% End function main parameter definitions. Repeat begin and end for as many functions you want to run

    main(function_list=fun_list, args_list=fun_args)
