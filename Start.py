#! python
from modules.Batches import BatchHandler
import datetime as dt
from modules import Validation


def main():
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


if __name__ == '__main__':
    main()
