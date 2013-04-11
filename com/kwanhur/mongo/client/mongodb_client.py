#!/usr/bin/env python
#_*_ code:utf-8 _*_
'''
Created on 2013-4-10

@author: huanghua
'''
import pymongo
from com.kwanhur.mongo.client.operator_symbol import QueryUpdateOperator
from com.kwanhur.mongo.client.operator_symbol import AggregationOperator


class QueryBuilder(object):
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
        self.key_val = {}
    
        
    def add_to_set(self, key, val):
        self.key_val[QueryUpdateOperator.ADD_TO_SET] = {key, val}
    
    def all(self, key, val_list):
        self.key_val[key] = {QueryUpdateOperator.ALL:val_list}
            
    def and_then(self, query_dic_list):
        self.key_val[QueryUpdateOperator.AND] = query_dic_list
    
    def bit(self, key, account):
        self.key_val[QueryUpdateOperator.BIT] = {key:{'and':account}}
    
    def each_add_to_set(self, key, val_list):
        self.key_val[QueryUpdateOperator.ADD_TO_SET] = {key:{QueryUpdateOperator.EACH:val_list}}
    
    def elem_match(self, key, query_dic_list):
        self.key_val['array'] = {QueryUpdateOperator.ELEM_MATCH:query_dic_list}
    
    def equals(self, key, val):
        self.key_val[key] = {AggregationOperator.EQ:val}
            
    def exists(self, key, val_boolean):
        self.key_val[key] = {QueryUpdateOperator.EXISTS:val_boolean}
        
    def greater_than(self, key, val):
        self.key_val[key] = {AggregationOperator.GT:val}    
    
    def greater_than_equals(self, key, val):
        self.key_val[key] = {AggregationOperator.GTE:val}
    
    def increase(self, key, account):
        self.key_val[QueryUpdateOperator.INC] = {key:account}
    
    def in_list(self, key, val_list):
        self.key_val[key] = {QueryUpdateOperator.IN:val_list}
        
    def less_than(self, key, val):       
        self.key_val[key] = {AggregationOperator.LT:val}
    
    def less_than_equals(self, key, val):
        self.key_val[key] = {AggregationOperator.LTE:val}
    
    def mod(self, key, divisor, remainder):
        self.key_val[key] = {QueryUpdateOperator.MOD:[divisor, remainder]}
    
    def nor(self, key, query_dic_list):
        self.key_val[QueryUpdateOperator.NOR] = query_dic_list
        
    def not_then(self, key, query_dic):
        self.key_val[key] = {QueryUpdateOperator.NOT:query_dic}
        
    def not_equals(self, key, val):
        self.key_val[key] = {AggregationOperator.NE:val}
    
    def not_in_list(self, key, val_list):
        self.key_val[key] = {QueryUpdateOperator.NIN:val_list}
        
    def or_then(self, key, query_dic_list):
        self.key_val[key] = {QueryUpdateOperator.OR:query_dic_list}
    
    def pop(self, key, val):
        self.key_val[QueryUpdateOperator.POP] = {key:val}
    
    def pull(self, key, val):
        self.key_val[QueryUpdateOperator.PULL] = {key:val}
    
    def pull_all(self, key, val_list):
        self.key_val[QueryUpdateOperator.PULL_ALL] = {key:val_list}
    
    def push(self, key, val):
        self.key_val[QueryUpdateOperator.PUSH] = {key:val}
    
    def push_all(self, key, val_list):
        self.key_val[QueryUpdateOperator.PUSH_ALL] = {key:val_list}
        
    '''
    set field value
    '''
    def put(self, key, val):
        self.key_val[key] = val
    
    def regex(self, key, val, options=None):
        if options == None:
            self.key_val[key] = {QueryUpdateOperator.REGEX:val}
        else:
            self.key_val[key] = {QueryUpdateOperator.REGEX:val, '$options':options}
    
    def rename(self, key_val_dic):
        self.key_val[QueryUpdateOperator.RENAME] = key_val_dic
    
    def set(self, key, val):
        self.key_val[QueryUpdateOperator.SET] = {key:val}
    
    def set_on_insert(self, key_val_dic):
        self.key_val[QueryUpdateOperator.SET_ON_INSERT] = {key_val_dic}
    
    def size(self, key, account):
        self.key_val[key] = {QueryUpdateOperator.SIZE:account}

class MongoDBClient(object):
    INITED = False
    CLOSED = False
    
    def __init__(self, ip, port, database_name, collection_name):
        self.ip = ip
        self.port = port
        self.database_name = database_name
        self.collection_name = collection_name
        
    def init(self):
        if self.INITED != True:
            self.client = pymongo.MongoClient(self.ip, self.port)
            self.database = self.client[self.database_name]
            self.collection = self.database[self.collection_name]
            self.INITED = True
        
    def close(self):
        if self.CLOSED != True:
            self.client.disconnect()
            self.CLOSED = True
    '''
    '''
    def select(self, query_dic, field_dic=None, pageindex=0, pagesize=10):
        self.init()
        if field_dic == None:
            cursor = self.collection.find(query_dic).skip(pageindex).limit(pagesize)
        elif field_dic != None:
            cursor = self.collection.find(query_dic, field_dic).skip(pageindex).limit(pagesize)        
        self.close()
        return cursor
    
    '''
    '''
    def select_sort(self, query_dic, field_dic=None, sort_key=None, sort_value=None, pageindex=0, pagesize=10):
        self.init()
        if field_dic == None and sort_key == None:
            cursor = self.collection.find(query_dic).skip(pageindex).limit(pagesize)
        elif field_dic != None and sort_key == None:
            cursor = self.collection.find(query_dic, field_dic).skip(pageindex).limit(pagesize)
        elif field_dic == None and sort_key != None:            
            cursor = self.collection.find(query_dic).sort(sort_key, sort_value).skip(pageindex).limit(pagesize)
        else:
            cursor = self.collection.find(query_dic, field_dic).sort(sort_key, sort_value).skip(pageindex).limit(pagesize)
        self.close()
        return cursor
    
    '''
    '''
    def select_one(self, query_dic, field_dic=None, sort_key=None, sort_value=None):
        if sort_key == None:
            cursor = self.select(query_dic, field_dic, 0, 1)
        else:
            cursor = self.select_sort(query_dic, field_dic, sort_key, sort_value, 0, 1)
        return cursor
    
    '''
    '''
    def select_count(self, query_dic):
        self.init()
        cursor = self.collection.find(query_dic).count()
        self.close()
        return cursor
    
    '''
    '''
    def insert(self, insert_dic):
        self.init()
        cursor = self.collection.insert(insert_dic)
        self.close()
        return cursor#return _id
    
    '''
    '''
    def insert_list(self, insert_dic_list):
        for insert_dic in insert_dic_list:
            self.insert(insert_dic)        
    
    '''
    '''
    def delete(self, query_dic=None):
        self.init()
        cursor = self.collection.remove(query_dic)
        self.close()
        if cursor != None:
            if cursor.get('ok') == 1.0:
                return True
        return False
    
    '''
    '''
    def update(self, query_dic, update_dic):
        self.init()
        cursor = self.collection.update(query_dic, update_dic)
        self.close()
        if cursor != None:
            if cursor.get('ok') == 1.0:
                return True
        return False   