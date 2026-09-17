"""
One MongoClient per Python process, reused across partitions and batches.

This lives in its own importable module on purpose. spark-submit runs
src/streaming/job.py as __main__, and cloudpickle copies everything a
__main__ function refers to BY VALUE when it ships a partition writer to the
Python workers. A client cache defined in job.py was therefore pickled too;
once the driver had opened a client (it holds thread locks), every batch
failed with "cannot pickle '_thread.lock' object" and the job restarted
every ~20 seconds. Functions from an importable module are pickled BY
REFERENCE, so each worker imports this module and keeps its own cache.
"""

from pymongo import MongoClient

_CLIENTS = {}


def client(uri: str) -> MongoClient:
    found = _CLIENTS.get(uri)
    if found is None:
        found = MongoClient(uri, serverSelectionTimeoutMS=10000)
        _CLIENTS[uri] = found
    return found
