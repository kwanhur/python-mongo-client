#!/usr/bin/env python
# _*_ coding:utf8 _*_
"""
Created on 2015-1-22

@author: huanghua
"""
from operator_symbol import *


class QueryBuilder(object):
    """
    builder for query
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

    def put(self, key, val):
        """
    set field value
    """
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
