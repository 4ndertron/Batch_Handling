#! python
from modules.Batches import BatchHandler

batches = BatchHandler(console_output=True)
batches.run_batch_handler()

# todo: Make a pandas export to a csv so the new account and transfer batch list can be stated and uploaded to
#   the warehouse
