import os
import pytest
from otterbrix import Client, DataBase, Collection, TypeIndex


database_name = "testdatabase_1"
collection_name = "testcollection_1"

client = Client(os.getcwd() + "/test_collection_index")
database = client[database_name]


def gen_id(num):
    res = str(num)
    while len(res) < 24:
        res = '0' + res
    return res


def insert(collection, num):
    new_obj = {
        '_id': gen_id(num),
        'count': num,
        'count_str': str(num),
        'count_bool': True if num & 1 else False
    }
    collection.insert(new_obj)


@pytest.fixture()
def gen_collection(request):
    collection = database[collection_name]
    collection.create_index(['count'], TypeIndex.SINGLE)
    collection.create_index(['count_str'], TypeIndex.SINGLE)
    for num in range(1000):
        insert(collection, num)

    def finalize():
        collection.drop()

    request.addfinalizer(finalizer=finalize)

    return {
        'collection': collection
    }


# def test_collection_find_by_one_index(gen_collection):
#     col = gen_collection['collection']
#     c = col.find({'count': {'$eq': 100}})
#     assert len(c) == 1
#     c.next()
#     assert c['count'] == 100
#     c.close()
#
#     c = col.find({'count': {'$eq': 1001}})
#     assert len(c) == 0
#     c.close()
#
#     c = col.find({'count': {'$gte': 100}})
#     assert len(c) == 900
#     c.close()
#
#     c = col.find({'count': {'$lt': 100}})
#     assert len(c) == 100
#     c.close()
#
#     c = col.find({'count': {'$and': [{'$lte': 100}, {'$gt': 50}]}})
#     assert len(c) == 50
#     c.close()
#
#
# def test_collection_find_by_two_index(gen_collection):
#     col = gen_collection['collection']
#     c = col.find({'$and': [
#         {'count': {'$eq': 100}},
#         {'count_str': {'$eq': '100'}}
#     ]})
#     assert len(c) == 1
#     c.next()
#     assert c['count'] == 100
#     c.close()
#
#     c = col.find({'$and': [
#         {'count': {'$eq': 100}},
#         {'count_str': {'$eq': '101'}}
#     ]})
#     assert len(c) == 0
#     c.close()
#
#     c = col.find({'$or': [
#         {'count': {'$eq': 100}},
#         {'count_str': {'$eq': '101'}}
#     ]})
#     assert len(c) == 2
#     c.close()
#
#
# def test_collection_find_by_one_index_after_insert(gen_collection):
#     col = gen_collection['collection']
#     c = col.find({'count': {'$eq': 1001}})
#     assert len(c) == 0
#     c.close()
#
#     insert(col, 1001)
#
#     c = col.find({'count': {'$eq': 1001}})
#     assert len(c) == 1
#     c.next()
#     assert c['count'] == 1001
#     c.close()
#
#
# def test_collection_find_by_one_index_after_delete(gen_collection):
#     col = gen_collection['collection']
#     c = col.find({'count': {'$gte': 500}})
#     assert len(c) == 500
#     c.close()
#
#     col.delete_one({'count': {'$gte': 500}})
#
#     c = col.find({'count': {'$gte': 500}})
#     assert len(c) == 499
#     c.close()
#
#     col.delete_many({'count': {'$gte': 500}})
#
#     c = col.find({'count': {'$gte': 500}})
#     assert len(c) == 0
#     c.close()
#
#
# def test_collection_find_by_one_index_after_update(gen_collection):
#     col = gen_collection['collection']
#     c = col.find({'count': {'$lt': 10}})
#     assert len(c) == 10
#     c.close()
#
#     col.update_one({'count': {'$gte': 10}}, {'$set': {'count': 0}})
#
#     c = col.find({'count': {'$lt': 10}})
#     assert len(c) == 11
#     c.close()
#
#     col.update_many({'count': {'$gte': 10}}, {'$set': {'count': 0}})
#
#     c = col.find({'count': {'$lt': 10}})
#     assert len(c) == 1000
#     c.close()
