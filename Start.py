#! python
import threading
import time
import datetime as dt
from modules import Validation
from modules.Batches import BatchHandler


def main_batch_process():
    if dt.date.today().isoweekday() == 3:
        batch_date = dt.date.today()
        save_location = Validation.batch_save_location_sharefile.value
    else:
        batch_date = dt.date.today() + dt.timedelta(days=-3)
        save_location = Validation.batch_save_location_local.value

    print(f'Batch date: {batch_date.strftime("%m.%d.%Y")}\nsave location: {save_location}\n')

    batches = BatchHandler(batch_end_date=batch_date,
                           save_location=save_location,
                           console_output=True)
    batches.run_batch_handler()


def upload_missing_file_types(missing_types):
    print(missing_types)
    if len(missing_types) > 0:
        for missing_type in missing_types:
            print(missing_type)
            parameters = {
                BatchHandler.Keys.MISSING_FILE_TYPE.value: missing_type,
                BatchHandler.Keys.PRIMARY_TABLE.value: Validation.file_type_primary_tables.value[missing_type]
            }
            batches = BatchHandler(console_output=True, **parameters)
            batches.run_missing_files()


def main():
    main_begin = time.time()

    main_batch_process_thread = threading.Thread(target=main_batch_process,
                                                 name='Wednesday Batch Processing')

    types = []
    types.append(Validation.missing_file_type_batch.value)
    types.append(Validation.missing_file_type_invoice.value)
    types.append(Validation.missing_file_type_continuation.value)

    missing_files_thread = threading.Thread(target=upload_missing_file_types,
                                            name='Uploading Files',
                                            args=(types,))

    if dt.date.today().isoweekday() == 3:
        main_batch_process_thread.start()
    else:
        missing_files_thread.start()

    while threading.active_count() > 1:
        print(f'{threading.active_count() - 1} extra processing running.\n')
        print(f'program runtime = {round(time.time() - main_begin, 2)} seconds.')
        time.sleep(5)


if __name__ == '__main__':
    main()
