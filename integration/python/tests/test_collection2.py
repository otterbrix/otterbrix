import copy
import os
import pytest
from otterbrix import Client, DataBase, Collection

database_name = "testdatabase"
collection_name = "testcollection"

client = Client(os.getcwd() + "/test_collection2") #todo host:port
database = client[database_name]
collection = database[collection_name]


def gen_id(num):
    res = str(num)
    while (len(res) < 24):
        res = '0' + res
    return res


@pytest.fixture()
def gen_collection(request):
    collection = database[collection_name]
    docs = []
    for num in range(100):
        obj = {}
        obj['_id'] = gen_id(num)
        obj['count'] = num
        obj['count_str'] = str(num)
        obj['countFloat'] = float(num) + 0.1
        obj['count_bool'] = True if num & 1 else False
        obj['count_array'] = [num + i for i in range(5)]
        obj['count_dict'] = {
            'odd': bool(num & 1),
            'even': not(num & 1),
            'three': not(num % 3),
            'five': not(num % 5),
        }
        obj['nested_array'] = [[num + i] for i in range(5)]
        obj['dict_array'] = [{'number': num + i} for i in range(5)]
        obj['mixed_dict'] = copy.deepcopy(obj)
        docs.append(obj)
    collection.insert_many(docs)

    def finalize():
        collection.drop()

    request.addfinalizer(finalizer=finalize)

    return {
        'collection': collection
    }


#todo
#def test_access_database_by_subscription():
#    assert client[database_name] == database


#todo
#def test_access_collection_by_subscription():
#    assert isinstance(database[collection_name], collection)


def test_list_collections():
    tables = database.collection_names()
    #assert '_default' in tables
    assert collection_name in tables


def test_initialize_collection(gen_collection):
    c = gen_collection['collection']
    cursor = c.find({})
    # count = 0
    # for doc in cursor:
    #     count += 1
    # assert count == 100
    assert cursor.count() == 100
    assert len(cursor) == 100
    assert c.count() == 100
    assert len(c) == 100
    assert len(c.find({})) == 100


def test_drop_collection(gen_collection):
    c = gen_collection['collection']
    assert database.drop_collection(collection_name) is True
    assert database.drop_collection(collection_name) is False
    c = database[collection_name]
    assert c.drop() is True
    assert c.drop() is False

def test_find_with_filter_named_parameter(gen_collection):
    c = gen_collection['collection'].find(filter={})
    assert c.count() == 100
    assert gen_collection['collection'].find(filter={}).count() == 100


def test_greater_than(gen_collection):
    c = gen_collection['collection'].find({'count': {'$gte': 50}})
    assert c.count() == 50
    assert gen_collection['collection'].find({'count': {'$gte': 50}}).count() == 50


def test_find_in_subdocument(gen_collection):
    c = gen_collection['collection'].find({'mixed_dict/count': 0})
    assert c.count() == 1
    assert gen_collection['collection'].find({'mixed_dict/count': 0}).count() == 1


def test_find_in_subdocument_with_operator(gen_collection):
    c = gen_collection['collection'].find({'mixed_dict/count': {'$gte': 50}})
    assert c.count() == 50
    assert gen_collection['collection'].find({'mixed_dict/count': {'$gte': 50}}).count() == 50


def test_find_in_subdocument_3_levels(gen_collection):
    c = gen_collection['collection'].find({'mixed_dict/count_dict/even': True})
    assert c.count() == 50
    assert gen_collection['collection'].find({'mixed_dict/count_dict/even': True}).count() == 50


def test_find_in_subdocument_with_array(gen_collection):
    c = gen_collection['collection'].find({'mixed_dict/count_array/3': {'$gt': 50}})
    assert c.count() == 52
    assert gen_collection['collection'].find({'mixed_dict/count_array/3': {"$gt": 50}}).count() == 52


def test_sort_positive(gen_collection):
    c = gen_collection['collection'].find()
    c.sort('count', 1)
    assert c[0]['count'] == 0
    assert c[1]['count'] == 1


def test_sort_negative(gen_collection):
    c = gen_collection['collection'].find()
    c.sort('count', -1)
    assert c[0]['count'] == 99
    assert c[1]['count'] == 98

def test_has_next(gen_collection):
    c = gen_collection['collection'].find().sort('count', 1)
    assert c.hasNext() is True
    assert c.next()['count'] == 0


def test_not_has_next(gen_collection):
    c = gen_collection['collection'].find({'count': {'$gte': 98}}).sort('count', 1)
    assert c.hasNext() is True
    assert c.next()['count'] == 98
    assert c.hasNext() is True
    assert c.next()['count'] == 99
    assert c.hasNext() is False


def test_empty_find(gen_collection):
    c = gen_collection['collection'].find()
    assert c.count() == 100


def test_find_one(gen_collection):
    c = gen_collection['collection'].find_one({'count': 3})
    assert c['count_str'] == '3'


def test_find_one_with_filter_named_parameter(gen_collection):
    c = gen_collection['collection'].find_one(filter={'count': 3})
    assert c['count_str'] == '3'

def test_gte_lt(gen_collection):
    c = gen_collection['collection'].find({'count': {'$gte': 50, '$lt': 51}})
    assert c.count() == 1
    assert c[0]['count_str'] == '50'


def test_gt_lte(gen_collection):
    c = gen_collection['collection'].find({'count': {'$gt': 50, '$lte': 51}})
    assert c.count() == 1
    assert c[0]['count_str'] == '51'


def test_ne(gen_collection):
    c = gen_collection['collection'].find({'count': {'$ne': 50}})
    assert c.count() == 99
    for item in c:
        assert item['count_str'] != '50'

def test_regex(gen_collection):
    c = gen_collection['collection'].find({'count_str': {'$regex': r'^[5]{1,2}'}}).sort('count', 1)
    assert c.count() == 11
    assert c[0]['count'] == 5
    assert c[1]['count'] == 50
    assert c[2]['count'] == 51
    assert c[10]['count'] == 59

    c = gen_collection['collection'].find({'count_str': {'$regex': r'[^5][5]{1}'}}).sort('count', 1)
    assert c.count() == 8
    assert c[0]['count'] == 15
    assert c[1]['count'] == 25
    assert c[4]['count'] == 65
    assert c[7]['count'] == 95


#def test_in(gen_collection):
    #c = gen_collection['collection'].find({'count': {'$in': [22,44,66,88]}}).sort('count', 1)
    #assert c.count() == 4
    #assert c[0]['count'] == 22
    #assert c[1]['count'] == 44
    #assert c[2]['count'] == 66
    #assert c[3]['count'] == 88

    #c = gen_collection['collection'].find({'count_str': {'$in': ['11','33','55','77','99']}}).sort('count', 1)
    #assert c.count() == 5
    #assert c[0]['count'] == 11
    #assert c[1]['count'] == 33
    #assert c[2]['count'] == 55
    #assert c[3]['count'] == 77
    #assert c[4]['count'] == 99

    #c = gen_collection['collection'].find({'count_array': {'$in': [22, 50]}}).sort('count', 1)
    #assert c.count() == 10
    #for doc in c:
        #if doc['count'] <= 22:
            #assert 22 in doc['count_array']
        #elif doc['count'] <= 50:
            #assert 50 in doc['count_array']


def test_update_one_set(gen_collection):
    c = gen_collection['collection'].update_one({'count': 3}, {'$set': {'count_str': 'three'}})
    assert len(c) == 1
    c.close()
    c = gen_collection['collection'].find_one({'count': 3})
    assert c['count_str'] == 'three'


def test_delete_one(gen_collection):
    gen_collection['collection'].delete_one({'count': 3})
    c = gen_collection['collection'].find({})
    assert c.count() == 99


def test_delete_all(gen_collection):
    gen_collection['collection'].delete_many({})
    c = gen_collection['collection'].find({})
    assert c.count() == 0


def test_delete_many(gen_collection):
    gen_collection['collection'].delete_many({'count': {'$gte': 50}})
    c = gen_collection['collection'].find({})
    assert c.count() == 50


def test_insert_one(gen_collection):
    gen_collection['collection'].insert_one({'my_object_name': 'my object value', 'count': 1000})
    c = gen_collection['collection'].find({})
    assert c.count() == 101
    # assert gen_collection['collection'].find({'my_object_name': 'my object value'})['count'] == 1000 #todo


def test_insert_many(gen_collection):
    items = []
    for i in range(10):
        value = 1000 + i
        items.append({'count': value, 'count_str': str(value)})
    gen_collection['collection'].insert_many(items)
    c = gen_collection['collection'].find({})
    assert c.count() == 110


def test_and(gen_collection):
    c = gen_collection['collection'].find({"$and": [{"count": {"$gt": 10}}, {"count": {"$lte": 50}}]})
    assert c.count() == 40


def test_or(gen_collection):
    c = gen_collection['collection'].find({"$or": [{"count": {"$lt": 10}}, {"count": {"$gte": 90}}]})
    assert c.count() == 20


# def test_not(gen_collection):
#     c = gen_collection['collection'].find({"count": {"$not": {"$gte": 90, "$lt": 10}}})
#     assert c.count() == 80


def test_delete_one(gen_collection):
    c = gen_collection['collection'].delete_one({'count_bool': True})
    assert c.count() == 1
    c.close()
    c = gen_collection['collection'].find({})
    assert c.count() == 99
    c.close()
    c = gen_collection['collection'].delete_one({'count_bool': True})
    assert c.count() == 1
    c.close()
    c = gen_collection['collection'].find({})
    assert c.count() == 98
    c.close()


def test_delete_all(gen_collection):
    c = gen_collection['collection'].delete_many({})
    assert c.count() == 100
    c.close()
    c = gen_collection['collection'].find({})
    assert c.count() == 0
    c.close()


def test_delete_many(gen_collection):
    c = gen_collection['collection'].delete_many({'count': {'$gte': 50}})
    assert c.count() == 50
    c.close()
    c = gen_collection['collection'].find({})
    assert c.count() == 50
    c.close()


def test_update_one(gen_collection):
    c = gen_collection['collection'].update_one({'count': {'$eq': 50}}, {'$set': {'count_str': '500'}})
    assert c.count() == 1
    c.close()
    c = gen_collection['collection'].find({'count': {'$eq': 50}})
    c.next()
    assert c['count_str'] == '500'
    c.close()


def test_update_many(gen_collection):
    c = gen_collection['collection'].update_many({'count': {'$gte': 50}}, {'$inc': {'count': 100}})
    assert c.count() == 50
    c.close()
    assert gen_collection['collection'].find({'count': {'$gt': 100}}).count() == 50


def test_update_with_add_new_field(gen_collection):
    assert gen_collection['collection'].find({'count_str2': {'$eq': '500'}}).count() == 0
    c = gen_collection['collection'].update_one({'count': {'$eq': 50}}, {'$set': {'count_str2': '500'}})
    assert c.count() == 1
    c.close()
    assert gen_collection['collection'].find({'count_str2': {'$eq': '500'}}).count() == 1


def test_update_with_upsert(gen_collection):
    c = gen_collection['collection'].update_one({'count': {'$eq': 100}}, {'$set': {'count_str': '500'}})
    assert c.count() == 0
    c.close()
    assert gen_collection['collection'].find({'count_str': {'$eq': '500'}}).count() == 0

    c = gen_collection['collection'].update_one({'count': {'$eq': 100}}, {'$set': {'count_str': '500'}}, True)
    assert c.count() == 1
    c.close()
    assert gen_collection['collection'].find({'count_str': {'$eq': '500'}}).count() == 1
