#! python
from modules.Batches import BatchHandler


def main():
    batches = BatchHandler(console_output=True)
    batches.run_batch_handler()


if __name__ == '__main__':
    main()
