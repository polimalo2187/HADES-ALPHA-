from __future__ import annotations

import os
import sys
import types

os.environ.setdefault('MONGODB_URI', 'mongodb://example.invalid/test')
os.environ.setdefault('DATABASE_NAME', 'test_db')

if 'pymongo' not in sys.modules:
    pymongo = types.ModuleType('pymongo')
    errors = types.ModuleType('pymongo.errors')

    class _DummyCollection:
        def __getitem__(self, name):
            return self

        def find_one(self, *args, **kwargs):
            return None

        def update_one(self, *args, **kwargs):
            return types.SimpleNamespace(modified_count=0)

        def update_many(self, *args, **kwargs):
            return types.SimpleNamespace(modified_count=0)

        def insert_one(self, *args, **kwargs):
            return types.SimpleNamespace(inserted_id='dummy')

        def find(self, *args, **kwargs):
            return []

        def find_one_and_update(self, *args, **kwargs):
            return None

        def count_documents(self, *args, **kwargs):
            return 0

        def aggregate(self, *args, **kwargs):
            return []

        def list_indexes(self):
            return []

        def create_indexes(self, *args, **kwargs):
            return []

        def create_index(self, *args, **kwargs):
            return None

    class MongoClient:
        def __init__(self, *args, **kwargs):
            pass

        def __getitem__(self, name):
            return _DummyCollection()

    class IndexModel:
        def __init__(self, keys, **kwargs):
            self.document = {'key': keys, **kwargs}

    class PyMongoError(Exception):
        pass

    class OperationFailure(PyMongoError):
        pass

    class ReturnDocument:
        AFTER = 'after'
        BEFORE = 'before'

    pymongo.ASCENDING = 1
    pymongo.DESCENDING = -1
    pymongo.IndexModel = IndexModel
    pymongo.MongoClient = MongoClient
    pymongo.ReturnDocument = ReturnDocument
    errors.PyMongoError = PyMongoError
    errors.OperationFailure = OperationFailure
    sys.modules['pymongo'] = pymongo
    sys.modules['pymongo.errors'] = errors
