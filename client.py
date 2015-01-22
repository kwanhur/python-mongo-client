#!/usr/bin/env python
# _*_ coding:utf8 _*_
"""
Created on 2013-4-10

@author: huanghua
"""
import os
import threading
import time

import gridfs
import pymongo
from pymongo.errors import OperationFailure


class MongoDBOperationException(Exception):
    pass


def lock_operation(func):
    def wrap_func(self, *args, **kwargs):
        try:
            if self.init:
                for i in xrange(3):
                    try:
                        if self.locked:
                            self.connecting_lock.acquire()
                        result = func(self, *args, **kwargs)
                        if self.locked:
                            self.connecting_lock.release()
                        return result
                    except OperationFailure:
                        time.sleep(0.02)
        except Exception, e:
            raise MongoDBOperationException(e)
        finally:
            self.close()

    return wrap_func


class MongoDBClient(object):
    """
        Constructor
        """

    def __init__(self, host, port, database=None, collection=None, safe=False, slave_okay=True, locked=False):
        self.host = host
        self.port = port
        self.db_name = database
        self.col_name = collection
        self.safe = safe
        self.slave_okay = slave_okay
        self.kwargs = {'connectTimeoutMS': 100000}  # 'socketTimeoutMS': 1000,

        self.client = None
        self.database = None
        self.collection = None

        self.connecting_lock = threading.Lock()
        self.locked = locked  # flag for using lock in thread connection

    def set_database(self, database):
        self.db_name = database

    def get_db(self):
        return pymongo.MongoClient(self.host, self.port)[self.db_name]

    def set_collection(self, collection):
        self.col_name = collection

    def get_collection(self):
        return self.get_db()[self.col_name]

    @property
    def init(self):
        """
    初始化操作
    """
        try:
            self.connecting_lock.acquire()
            if not self.client:
                self.client = pymongo.MongoClient(self.host, self.port, **self.kwargs)
            if self.client:
                self.client.slave_okay = self.slave_okay
                self.client.safe = self.safe
                self.database = self.client[self.db_name]
                self.collection = self.database[self.col_name]
            return True
        except Exception, e:
            raise Exception(e)
        finally:
            self.connecting_lock.release()

    def close(self):
        """
    关闭操作
    """
        try:
            self.connecting_lock.acquire()
            if self.database and self.database.connection:
                self.database.connection.disconnect()
                self.database = None
            if self.client:
                self.client.disconnect()
                self.client = None
            self.connecting_lock.release()
        except Exception, e:
            raise Exception(e)

    def select_list(self, query_dic, field_dic=None, page_index=None, page_size=None):
        data_list = []
        cursor = self.select(query_dic, field_dic, page_index, page_size)
        if cursor:
            data_list = list(cursor)
        return data_list

    @lock_operation
    def select(self, query_dic, field_dic=None, page_index=None, page_size=None):
        """
    query_dic 查询条件
    field_dic 字段返回条件 如{'_id':0}
    page_index 页码
    page_size 页大小
    """
        cursor = None
        if field_dic is None:
            if page_index >= 0 and page_size:
                cursor = self.collection.find(query_dic).skip(page_index).limit(page_size)
            elif page_index >= 0:
                cursor = self.collection.find(query_dic).skip(page_index)
            elif page_size:
                cursor = self.collection.find(query_dic).limit(page_size)
            else:
                cursor = self.collection.find(query_dic)
        elif field_dic:
            if page_index >= 0 and page_size:
                cursor = self.collection.find(query_dic, field_dic).skip(page_index).limit(page_size)
            elif page_index >= 0:
                cursor = self.collection.find(query_dic, field_dic).skip(page_index)
            elif page_size:
                cursor = self.collection.find(query_dic, field_dic).limit(page_size)
            else:
                cursor = self.collection.find(query_dic, field_dic)
        return cursor

    def select_sort_list(self, query_dic, field_dic=None, sort_key=None, sort_value=None, page_index=None,
                         page_size=None):
        data_list = []
        cursor = self.select_sort(query_dic, field_dic, sort_key, sort_value, page_index, page_size)
        if cursor:
            data_list = list(cursor)
        return data_list

    @lock_operation
    def select_sort(self, query_dic, field_dic=None, sort_key=None, sort_value=None, page_index=None, page_size=None):
        """
    query_dic 查询条件
    field_dic 字段返回条件 如{'_id':0}
    sort_key 排序字段，如sort_key='_id'
    sort_value 排序字段值 如sort_value=1
    page_index 页码
    page_size 页大小
    """
        if field_dic is None and sort_key is None:
            if page_index >= 0 and page_size:
                cursor = self.collection.find(query_dic).skip(page_index).limit(page_size)
            elif page_index >= 0:
                cursor = self.collection.find(query_dic).skip(page_index)
            elif page_size:
                cursor = self.collection.find(query_dic).limit(page_size)
            else:
                cursor = self.collection.find(query_dic)
        elif field_dic and sort_key is None:
            if page_index >= 0 and page_size:
                cursor = self.collection.find(query_dic, field_dic).skip(page_index).limit(page_size)
            elif page_index >= 0:
                cursor = self.collection.find(query_dic, field_dic).skip(page_index)
            elif page_size:
                cursor = self.collection.find(query_dic, field_dic).limit(page_size)
            else:
                cursor = self.collection.find(query_dic, field_dic)
        elif field_dic is None and sort_key:
            if page_index >= 0 and page_size:
                if isinstance(sort_key, list):  # [(,)(,)]
                    cursor = self.collection.find(query_dic).sort(sort_key).skip(page_index).limit(page_size)
                else:
                    cursor = self.collection.find(query_dic).sort(sort_key, sort_value).skip(page_index).limit(
                        page_size)
            elif page_index >= 0:
                if isinstance(sort_key, list):  # [(,)(,)]
                    cursor = self.collection.find(query_dic).sort(sort_key).skip(page_index)
                else:
                    cursor = self.collection.find(query_dic).sort(sort_key, sort_value).skip(page_index)
            elif page_size:
                if isinstance(sort_key, list):  # [(,)(,)]
                    cursor = self.collection.find(query_dic).sort(sort_key).limit(page_size)
                else:
                    cursor = self.collection.find(query_dic).sort(sort_key, sort_value).limit(page_size)
            else:
                if isinstance(sort_key, list):  # [(,)(,)]
                    cursor = self.collection.find(query_dic).sort(sort_key)
                else:
                    cursor = self.collection.find(query_dic).sort(sort_key, sort_value)
        else:
            if page_index >= 0 and page_size:
                if isinstance(sort_key, list):  # [(,)(,)]
                    cursor = self.collection.find(query_dic, field_dic).sort(sort_key).skip(page_index).limit(
                        page_size)
                else:
                    cursor = self.collection.find(query_dic, field_dic).sort(sort_key, sort_value).skip(
                        page_index).limit(page_size)
            elif page_index >= 0:
                if isinstance(sort_key, list):  # [(,)(,)]
                    cursor = self.collection.find(query_dic, field_dic).sort(sort_key).skip(page_index)
                else:
                    cursor = self.collection.find(query_dic, field_dic).sort(sort_key, sort_value).skip(
                        page_index)
            elif page_size:
                if isinstance(sort_key, list):  # [(,)(,)]
                    cursor = self.collection.find(query_dic, field_dic).sort(sort_key).limit(page_size)
                else:
                    cursor = self.collection.find(query_dic, field_dic).sort(sort_key, sort_value).limit(
                        page_size)
            else:
                if isinstance(sort_key, list):  # [(,)(,)]
                    cursor = self.collection.find(query_dic, field_dic).sort(sort_key)
                else:
                    cursor = self.collection.find(query_dic, field_dic).sort(sort_key, sort_value)
        return cursor

    def select_one_dic(self, query_dic, field_dic=None, sort_key=None, sort_value=None):
        data_dic = {}
        cursor = self.select_one(query_dic, field_dic, sort_key, sort_value)
        if cursor:
            cursor_list = list(cursor)
            if cursor_list:
                data_dic = cursor_list[0]
        return data_dic

    def select_one(self, query_dic, field_dic=None, sort_key=None, sort_value=None):
        """
    query_dic 查询条件
    field_dic 字段返回条件 如{'_id':0}
    sort_key 排序字段，如sort_key='_id'
    sort_value 排序字段值 如sort_value=1
    """
        if sort_key is None:
            cursor = self.select(query_dic, field_dic, 0, 1)
        else:
            cursor = self.select_sort(query_dic, field_dic, sort_key, sort_value, 0, 1)
        return cursor

    @lock_operation
    def select_count(self, query_dic):
        """
    query_dic 查询条件
    返回条数，如0,1等
    """
        if not query_dic:
            return self.collection.count()
        return self.collection.find(query_dic).count()

    @lock_operation
    def insert(self, insert_dic):
        """
    insert_dic 插入的数据
    返回_id
    """
        return self.collection.insert(insert_dic)

    @DeprecationWarning
    def insert_list(self, insert_dic_list):
        """
        insert_dic_list 插入的数据列表
        列表循环插入
        """
        for insert_dic in insert_dic_list:
            self.insert(insert_dic)

    @lock_operation
    def insert_bulk(self, insert_list):
        """
        批量插入insert_list
        """
        return self.collection.insert(insert_list)

    @lock_operation
    def delete(self, query_dic=None):
        """
        删除
        query_dic 查询条件
        """
        return self.collection.remove(query_dic) is not None

    @lock_operation
    def update(self, query_dic, update_dic):
        """
        更新
        query_dic 查询条件
        update_dic 更新的数据
        """
        cursor = self.collection.update(query_dic, update_dic)
        if cursor:
            if cursor.get('ok') == 1.0:
                return True
        return False

    @lock_operation
    def upsert(self, query_dic, update_dic):
        """
        存在则更新，不存在则插入
        query_dic 查询条件
        update_dic 更新的数据
        """
        cursor = self.collection.update(query_dic, update_dic, upsert=True)
        if cursor:
            if cursor.get('ok') == 1.0:
                return True
        return False

    def aggregate_list(self, pipeline):
        data_list = []
        cursor = self.aggregate(pipeline)
        if cursor:
            data_list = cursor['result']
        return data_list

    @lock_operation
    def aggregate(self, pipeline):
        """
        聚合函数
        """
        return self.collection.aggregate(pipeline)

    def get_database_names(self):
        """
        获取服务器所有库名
        """
        try:
            if self.init:
                return self.client.database_names()
        finally:
            self.close()

    def run_command(self, command):
        """
        运行mongodb命令
        """
        try:
            if self.init:
                return self.database.command(command)
        finally:
            self.close()

    def get_collection_names(self):
        """
        获取库中所有表名
        """
        try:
            if self.init:
                return self.database.collection_names()
        finally:
            self.close()

    @lock_operation
    def ensure_index(self, key_or_list):
        """
            a single key or a list of (key, direction)
        """
        return self.collection.ensure_index(key_or_list)

    @lock_operation
    def drop_all_index(self):
        """
        删除所有索引
        """
        return self.collection.drop_indexes()

    @lock_operation
    def drop_index(self, index_or_name):
        """
        根据索引名称删除
        """
        return self.collection.drop_index(index_or_name)

    @lock_operation
    def get_all_index(self):
        """
        获取所有已创建的索引
        """
        return self.collection.index_information()

    @lock_operation
    def map_reduce(self, map_function, reduce_function, full_response=False, **kwargs):
        """
        运行mapreduce
        """
        return self.collection.map_reduce(map_function, reduce_function, full_response, kwargs)

    @lock_operation
    def write_grid_fs(self, file_full_path, encoding='utf-8', content_type='text/plain', chunk_size=256 * 1024):
        """
        :param file_full_path:
        :param encoding:
        :param content_type:
        :param chunk_size:
        :return:
        """
        grid_fs = gridfs.GridFS(self.database)
        data_dic = {'filename': os.path.basename(file_full_path), 'encoding': encoding,
                    'content_type': content_type, 'chunk_size': chunk_size}
        grid_in = grid_fs.new_file(**data_dic)
        try:
            grid_in.write(self.read_file(file_full_path))
            flag = True
        finally:
            if grid_in:
                grid_in.close()
        return flag

    @lock_operation
    def select_sort_grid_fs(self, query_dic, sort_key=None, sort_value=None, page_index=None, page_size=None,
                            timeout=True):
        """
        :param query_dic:
        :param sort_key:
        :param sort_value:
        :param page_index:
        :param page_size:
        :return:
        """
        grid_fs = gridfs.GridFS(self.database)
        if sort_key is None:
            if page_index is None:
                grid_out_cursor = grid_fs.find(query_dic, timeout=timeout)
            else:
                grid_out_cursor = grid_fs.find(query_dic, timeout=timeout).skip(page_index).limit(page_size)
        else:
            if page_index is None:
                if isinstance(sort_key, list):
                    grid_out_cursor = grid_fs.find(query_dic, timeout=timeout).sort(sort_key)
                else:
                    grid_out_cursor = grid_fs.find(query_dic, timeout=timeout).sort(sort_key, sort_value)
            else:
                if isinstance(sort_key, list):
                    grid_out_cursor = grid_fs.find(query_dic, timeout=timeout).sort(sort_key).skip(
                        page_index).limit(page_size)
                else:
                    grid_out_cursor = grid_fs.find(query_dic, timeout=timeout).sort(sort_key, sort_value).skip(
                        page_index).limit(page_size)
        return grid_out_cursor

    @lock_operation
    def select_grid_fs_files_list(self, query_dic, sort_key=None, sort_value=None, page_index=None, page_size=None):
        """
        :param query_dic: ex:{'filename':'xx.txt'}
        :return:
        """
        if not query_dic:
            return None
        data_dic_list = []
        grid_out_list = self.select_sort_grid_fs(query_dic, sort_key=sort_key, sort_value=sort_value,
                                                 page_index=page_index, page_size=page_size)
        if grid_out_list:
            data_dic_list = [data._file for data in grid_out_list]
        return data_dic_list

    def select_one_grid_fs_files(self, query_dic, sort_key=None, sort_value=None):
        """
        :param query_dic: ex:{'filename':'xx.txt'}
        :return:
        """
        if not query_dic:
            return None
        try:
            grid_out_list = self.select_grid_fs_files_list(query_dic, sort_key=sort_key, sort_value=sort_value,
                                                           page_index=0, page_size=1)
            if grid_out_list:
                return grid_out_list[0]
        finally:
            self.close()

    @lock_operation
    def select_grid_fs_chunks_list(self, query_dic, sort_key=None, sort_value=None, page_index=None, page_size=None):
        """
        :param query_dic: ex:{'filename':'xx.txt'}
        :return:
        """
        if not query_dic:
            return None
        data_list = []
        grid_out_list = self.select_sort_grid_fs(query_dic, sort_key=sort_key, sort_value=sort_value,
                                                 page_index=page_index, page_size=page_size)
        if grid_out_list:
            data_list = [grid_out.read() for grid_out in grid_out_list]
        return data_list

    def select_one_grid_fs_chunks(self, query_dic, sort_key=None, sort_value=None):
        """
        :param query_dic: ex:{'filename':'xx.txt'}
        :return:
        """
        if not query_dic:
            return None
        grid_out_list = self.select_grid_fs_chunks_list(query_dic, sort_key=sort_key, sort_value=sort_value,
                                                        page_index=0, page_size=1)
        if grid_out_list:
            return grid_out_list[0]

    @lock_operation
    def remove_grid_fs(self, query_dic):
        """
        :param query_dic:
        :return:
        """
        grid_fs_files_list = self.select_grid_fs_files_list(query_dic)
        if grid_fs_files_list:
            grid_fs = gridfs.GridFS(self.database)
            [grid_fs.delete(grid_fs_file['_id']) for grid_fs_file in grid_fs_files_list]

    @lock_operation
    def remove_oldest_grid_fs(self, query_dic):
        """
        :param query_dic:
        :return:
        """
        grid_fs_file = self.select_one_grid_fs_files(query_dic, sort_key='uploadDate', sort_value=1)
        if grid_fs_file:
            grid_fs = gridfs.GridFS(self.database)
            grid_fs.delete(grid_fs_file['_id'])

    @lock_operation
    def remove_one_grid_fs(self, file_id):
        """
        :param file_id:
        :return:
        """
        if not file_id:
            return None
        grid_fs = gridfs.GridFS(self.database)
        grid_fs.delete(file_id)

    @staticmethod
    def read_file(file_path):
        """
        读文件
    """
        text = ""
        with open(file_path, 'r') as f:
            lines = f.readlines()
            for line in lines:
                text += line
        return text


if __name__ == '__main__':
    pass