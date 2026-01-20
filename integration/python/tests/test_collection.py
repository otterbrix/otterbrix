import copy
import os
import pytest
from otterbrix import Client, DataBase, Collection

client = Client(os.getcwd() + "/test_collection")
friedrich_database = client["friedrichdatabase"]
friedrich_collection = friedrich_database["friedrichcollection"]

def gen_id(num):
    res = str(num)
    while (len(res) < 24):
        res = '0' + res
    return res

for num in range(50):
    new_obj = {}
    new_obj['_id'] = gen_id(num)
    new_obj['count'] = num
    new_obj['count_str'] = str(num)
    new_obj['countFloat'] = float(num) + 0.1
    new_obj['count_bool'] = True if num & 1 else False
    new_obj['count_array'] = [num + i for i in range(5)]
    new_obj['count_dict'] = {
        'odd': bool(num & 1),
        'even': not (num & 1),
        'three': not (num % 3),
        'five': not (num % 5),
    }
    new_obj['nested_array'] = [[num + i] for i in range(5)]
    new_obj['dict_array'] = [{'number': num + i} for i in range(5)]
    new_obj['mixed_dict'] = copy.deepcopy(new_obj)
    friedrich_collection.insert(new_obj)

list_doc = []
for num in range(50,100):
    new_obj = {}
    new_obj['_id'] = gen_id(num)
    new_obj['count'] = num
    new_obj['count_str'] = str(num)
    new_obj['countFloat'] = float(num) + 0.1
    new_obj['count_bool'] = True if num & 1 else False
    new_obj['count_array'] = [num + i for i in range(5)]
    new_obj['count_dict'] = {
        'odd': bool(num & 1),
        'even': not (num & 1),
        'three': not (num % 3),
        'five': not (num % 5),
    }
    new_obj['nested_array'] = [[num + i] for i in range(5)]
    new_obj['dict_array'] = [{'number': num + i} for i in range(5)]
    new_obj['mixed_dict'] = copy.deepcopy(new_obj)
    list_doc.append(new_obj)
friedrich_collection.insert(list_doc)

list_doc = []
for num in range(100):
    new_obj = {}
    new_obj['_id'] = gen_id(num)
    list_doc.append(new_obj)
friedrich_collection.insert(list_doc) # not inserted (not unique id)


def test_collection_len():
    assert len(friedrich_collection) == 100
    assert len(friedrich_database['friedrichcollection']) == 100

def test_collection_find():
    c = friedrich_collection.find({})
    assert len(c) == 100
    c.close()

    c = friedrich_collection.find({'count': {'$gt': 90}})
    assert len(c) == 9
    c.close()

    c = friedrich_collection.find({'count_str': {'$regex': '9$'}})
    assert len(c) == 10
    c.close()

    c = friedrich_collection.find({'$or': [{'count': {'$gt': 90}}, {'count_str': {'$regex': '9$'}}]})
    assert len(c) == 18
    c.close()

    c = friedrich_collection.find({'$and': [{'$or': [{'count': {'$gt': 90}}, {'count_str': {'$regex': '9$'}}]}, {'count': {'$lte': 30}}]})
    assert len(c) == 3
    c.close()

def test_collection_cursor():
    c = friedrich_collection.find({})
    count = 0
    while c.hasNext():
        c.next()
        assert str(c['count']) == c['count_str']
        assert c.hasNext() == (count < 99)
        count += 1
    c.close()


def test_collection_find_one():
    c = friedrich_collection.find_one({'_id': {'$eq': gen_id(1)}})
    assert c['count'] == 1
    c = friedrich_collection.find_one({'count': {'$eq': 10}})
    assert c['count'] == 10
    c = friedrich_collection.find_one({'$and': [{'count': {'$gt': 90}}, {'count_str': {'$regex': '9$'}}]})
    assert c['count'] == 99