#!/usr/bin/env python
#_*_ coding:utf8 _*_
"""
Created on 2013-4-10

@author: huanghua
"""
import pymongo
from operator_symbol import *


class QueryBuilder(object):
    """
    classdocs
    """

    def __init__(self):
        """
        Constructor
        """
        self.key_val = {}

    def add_to_set(self, key, val):
        self.key_val[QueryUpdateOperator.ADD_TO_SET] = {key, val}
        return self

    def all(self, key, val_list):
        self.key_val[key] = {QueryUpdateOperator.ALL: val_list}
        return self

    def and_then(self, query_dic_list):
        self.key_val[QueryUpdateOperator.AND] = query_dic_list
        return self

    def bit(self, key, account):
        self.key_val[QueryUpdateOperator.BIT] = {key: {'and': account}}
        return self

    def each_add_to_set(self, key, val_list):
        self.key_val[QueryUpdateOperator.ADD_TO_SET] = {key: {QueryUpdateOperator.EACH: val_list}}
        return self

    def elem_match(self, key, query_dic_list):
        self.key_val['array'] = {QueryUpdateOperator.ELEM_MATCH: query_dic_list}
        return self

    def equals(self, key, val):
        self.key_val[key] = {AggregationOperator.EQ: val}
        return self

    def exists(self, key, val_boolean):
        self.key_val[key] = {QueryUpdateOperator.EXISTS: val_boolean}
        return self

    def greater_than(self, key, val):
        self.key_val[key] = {AggregationOperator.GT: val}
        return self

    def greater_than_equals(self, key, val):
        self.key_val[key] = {AggregationOperator.GTE: val}
        return self

    def increase(self, key, account):
        self.key_val[QueryUpdateOperator.INC] = {key: account}
        return self

    def in_list(self, key, val_list):
        self.key_val[key] = {QueryUpdateOperator.IN: val_list}
        return self

    def less_than(self, key, val):
        self.key_val[key] = {AggregationOperator.LT: val}
        return self

    def less_than_equals(self, key, val):
        self.key_val[key] = {AggregationOperator.LTE: val}
        return self

    def mod(self, key, divisor, remainder):
        self.key_val[key] = {QueryUpdateOperator.MOD: [divisor, remainder]}
        return self

    def nor(self, key, query_dic_list):
        self.key_val[key] = {QueryUpdateOperator.NOR, query_dic_list}
        return self

    def not_then(self, key, query_dic):
        self.key_val[key] = {QueryUpdateOperator.NOT: query_dic}
        return self

    def not_equals(self, key, val):
        self.key_val[key] = {AggregationOperator.NE: val}
        return self

    def not_in_list(self, key, val_list):
        self.key_val[key] = {QueryUpdateOperator.NIN: val_list}
        return self

    def or_then(self, key, query_dic_list):
        self.key_val[key] = {QueryUpdateOperator.OR: query_dic_list}
        return self

    def pop(self, key, val):
        self.key_val[QueryUpdateOperator.POP] = {key: val}
        return self

    def pull(self, key, val):
        self.key_val[QueryUpdateOperator.PULL] = {key: val}
        return self

    def pull_all(self, key, val_list):
        self.key_val[QueryUpdateOperator.PULL_ALL] = {key: val_list}
        return self

    def push(self, key, val):
        self.key_val[QueryUpdateOperator.PUSH] = {key: val}
        return self

    def push_all(self, key, val_list):
        self.key_val[QueryUpdateOperator.PUSH_ALL] = {key: val_list}
        return self

    '''
    set field value
    '''

    def put(self, key, val):
        self.key_val[key] = val
        return self

    def regex(self, key, val, options=None):
        if options is None:
            self.key_val[key] = {QueryUpdateOperator.REGEX: val}
        else:
            self.key_val[key] = {QueryUpdateOperator.REGEX: val, '$options': options}
        return self

    def rename(self, key_val_dic):
        self.key_val[QueryUpdateOperator.RENAME] = key_val_dic
        return self

    def set(self, key, val):
        self.key_val[QueryUpdateOperator.SET] = {key: val}
        return self

    def set_on_insert(self, key_val_dic):
        self.key_val[QueryUpdateOperator.SET_ON_INSERT] = {key_val_dic}
        return self

    def size(self, key, account):
        self.key_val[key] = {QueryUpdateOperator.SIZE: account}
        return self


class MongoDBClient(object):
    INITED = False #标识连接是否已经初始化
    CLOSED = False #标识连接是否已经关闭

    """
        Constructor
        """

    def __init__(self, host, port, database=None, collection=None, safe=False, slave_okay=False):
        self.host = host
        self.port = port
        self.database = database
        self.collection = collection
        self.safe = safe
        self.slave_okay = slave_okay

    def set_database(self, database):
        self.database = database

    def get_db(self):
        return pymongo.MongoClient(self.host, self.port)[self.database]

    def set_collection(self, collection):
        self.collection = collection

    def get_collection(self):
        return self.get_db()[self.collection]

    """
    初始化操作
    """

    def init(self):
        if not self.INITED:
            self.client = pymongo.MongoClient(self.host, self.port)
            if self.client:
                self.database = self.client[self.database]
                self.collection = self.database[self.collection]
                self.collection.slave_okay = self.slave_okay
                self.collection.safe = self.safe
                self.INITED = True

    """
    关闭操作
    """

    def close(self):
        if not self.CLOSED and self.client:
            self.client.disconnect()
            self.CLOSED = True

    '''
    query_dic 查询条件
    field_dic 字段返回条件 如{'_id':0}
    page_index 页码
    page_size 页大小
    '''

    def select(self, query_dic, field_dic=None, page_index=None, page_size=None):
        try:
            self.init()
            cursor = None
            if field_dic is None:
                if page_index and page_size:
                    cursor = self.collection.find(query_dic).skip(page_index).limit(page_size)
                elif page_index:
                    cursor = self.collection.find(query_dic).skip(page_index)
                elif page_size:
                    cursor = self.collection.find(query_dic).limit(page_size)
                else:
                    cursor = self.collection.find(query_dic)
            elif field_dic:
                if page_index and page_size:
                    cursor = self.collection.find(query_dic, field_dic).skip(page_index).limit(page_size)
                elif page_index:
                    cursor = self.collection.find(query_dic, field_dic).skip(page_index)
                elif page_size:
                    cursor = self.collection.find(query_dic, field_dic).limit(page_size)
                else:
                    cursor = self.collection.find(query_dic, field_dic)
            return cursor
        finally:
            self.close()

    '''
    query_dic 查询条件
    field_dic 字段返回条件 如{'_id':0}
    sort_key 排序字段，如sort_key='_id'
    sort_value 排序字段值 如sort_value=1
    page_index 页码
    page_size 页大小
    '''

    def select_sort(self, query_dic, field_dic=None, sort_key=None, sort_value=None, page_index=None, page_size=None):
        try:
            self.init()
            if field_dic is None and sort_key is None:
                if page_index and page_size:
                    cursor = self.collection.find(query_dic).skip(page_index).limit(page_size)
                elif page_index:
                    cursor = self.collection.find(query_dic).skip(page_index)
                elif page_size:
                    cursor = self.collection.find(query_dic).limit(page_size)
                else:
                    cursor = self.collection.find(query_dic)
            elif field_dic and sort_key is None:
                if page_index and page_size:
                    cursor = self.collection.find(query_dic, field_dic).skip(page_index).limit(page_size)
                elif page_index:
                    cursor = self.collection.find(query_dic, field_dic).skip(page_index)
                elif page_size:
                    cursor = self.collection.find(query_dic, field_dic).limit(page_size)
                else:
                    cursor = self.collection.find(query_dic, field_dic)
            elif field_dic is None and sort_key:
                if page_index and page_size:
                    if isinstance(sort_key, list):#[(,)(,)]
                        cursor = self.collection.find(query_dic).sort(sort_key).skip(page_index).limit(page_size)
                    else:
                        cursor = self.collection.find(query_dic).sort(sort_key, sort_value).skip(page_index).limit(
                            page_size)
                elif page_index:
                    if isinstance(sort_key, list):#[(,)(,)]
                        cursor = self.collection.find(query_dic).sort(sort_key).skip(page_index)
                    else:
                        cursor = self.collection.find(query_dic).sort(sort_key, sort_value).skip(page_index)
                elif page_size:
                    if isinstance(sort_key, list):#[(,)(,)]
                        cursor = self.collection.find(query_dic).sort(sort_key).limit(page_size)
                    else:
                        cursor = self.collection.find(query_dic).sort(sort_key, sort_value).limit(page_size)
                else:
                    if isinstance(sort_key, list):#[(,)(,)]
                        cursor = self.collection.find(query_dic).sort(sort_key)
                    else:
                        cursor = self.collection.find(query_dic).sort(sort_key, sort_value)
            else:
                if page_index and page_size:
                    if isinstance(sort_key, list):#[(,)(,)]
                        cursor = self.collection.find(query_dic, field_dic).sort(sort_key).skip(page_index).limit(
                            page_size)
                    else:
                        cursor = self.collection.find(query_dic, field_dic).sort(sort_key, sort_value).skip(
                            page_index).limit(
                            page_size)
                elif page_index:
                    if isinstance(sort_key, list):#[(,)(,)]
                        cursor = self.collection.find(query_dic, field_dic).sort(sort_key).skip(page_index)
                    else:
                        cursor = self.collection.find(query_dic, field_dic).sort(sort_key, sort_value).skip(page_index)
                elif page_size:
                    if isinstance(sort_key, list):#[(,)(,)]
                        cursor = self.collection.find(query_dic, field_dic).sort(sort_key).limit(page_size)
                    else:
                        cursor = self.collection.find(query_dic, field_dic).sort(sort_key, sort_value).limit(page_size)
                else:
                    if isinstance(sort_key, list):#[(,)(,)]
                        cursor = self.collection.find(query_dic, field_dic).sort(sort_key)
                    else:
                        cursor = self.collection.find(query_dic, field_dic).sort(sort_key, sort_value)
            return cursor
        finally:
            self.close()

    '''
    query_dic 查询条件
    field_dic 字段返回条件 如{'_id':0}
    sort_key 排序字段，如sort_key='_id'
    sort_value 排序字段值 如sort_value=1
    '''

    def select_one(self, query_dic, field_dic=None, sort_key=None, sort_value=None):
        if sort_key is None:
            cursor = self.select(query_dic, field_dic, 0, 1)
        else:
            cursor = self.select_sort(query_dic, field_dic, sort_key, sort_value, 0, 1)
        return cursor

    '''
    query_dic 查询条件
    返回条数，如0,1等
    '''

    def select_count(self, query_dic):
        try:
            self.init()
            cursor = self.collection.find(query_dic).count()
            return cursor
        finally:
            self.close()

    '''
    insert_dic 插入的数据
    返回_id
    '''

    def insert(self, insert_dic):
        try:
            self.init()
            cursor = self.collection.insert(insert_dic)
            return cursor
        finally:
            self.close()

    '''
    insert_dic_list 插入的数据列表
    列表循环插入
    '''

    def insert_list(self, insert_dic_list):
        for insert_dic in insert_dic_list:
            self.insert(insert_dic)

    """
    批量插入insert_list
    """

    def insert_bulk(self, insert_list):
        try:
            self.init()
            cursor = self.collection.insert(insert_list)
            return cursor
        finally:
            self.close()

    '''
    删除
    query_dic 查询条件
    '''

    def delete(self, query_dic=None):
        try:
            self.init()
            cursor = self.collection.remove(query_dic)
            if cursor:
                if cursor.get('ok') == 1.0:
                    return True
            return False
        finally:
            self.close()

    '''
    更新
    query_dic 查询条件
    update_dic 更新的数据
    '''

    def update(self, query_dic, update_dic):
        try:
            self.init()
            cursor = self.collection.update(query_dic, update_dic)
            if cursor:
                if cursor.get('ok') == 1.0:
                    return True
            return False
        finally:
            self.close()

    """
    存在则更新，不存在则插入
    query_dic 查询条件
    update_dic 更新的数据
    """

    def upsert(self, query_dic, update_dic):
        try:
            self.init()
            cursor = self.collection.update(query_dic, update_dic, upsert=True)
            if cursor:
                if cursor.get('ok') == 1.0:
                    return True
            return False
        finally:
            self.close()

    """
    聚合函数
    """

    def aggregate(self, pipeline):
        try:
            self.init()
            cursor = self.collection.aggregate(pipeline)
            return cursor
        finally:
            self.close()

    """
    获取服务器所有库名
    """

    def get_database_names(self):
        return self.client.database_names()

    """
    运行mongodb命令
    """

    def run_command(self, command):
        return self.database.command(command)

    """
    获取库中所有表名
    """

    def get_collection_names(self):
        return self.database.collection_names()

    """
        a single key or a list of (key, direction)
    """

    def ensure_index(self, key_or_list):
        return self.collection.ensure_index(key_or_list)

    """
    删除所有索引
    """

    def drop_all_index(self):
        return self.collection.drop_indexes()

    """
    根据索引名称删除
    """

    def drop_index(self, index_or_name):
        return self.collection.drop_index(index_or_name)

    """
    获取所有已创建的索引
    """

    def get_all_index(self):
        return self.collection.index_information()

    """
    运行mapreduce
    """

    def map_reduce(self, map_function, reduce_function, full_response=False, **kwargs):
        return self.collection.map_reduce(map_function, reduce_function, full_response, kwargs)


if __name__ == '__main__':
    conn = pymongo.Connection(host='192.168.46.13', port=10001)
    db_list = conn.database_names()
    db = conn['data_2013']
    collection_list = db.collection_names()