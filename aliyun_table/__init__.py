"""
This module provide operations related to Aliyun table storage.
"""

import datetime
import os
import time
from copy import deepcopy
from logging import getLogger, INFO, basicConfig
from pprint import pprint

from prettytable import PrettyTable
from tablestore import *
from tablestore import OTSClient, Row, Condition, WriteRetryPolicy
from tablestore.metadata import RowExistenceExpectation
from tablestore.retry import NoRetryPolicy

from aliyun_table.my_logger import logger


def _get_md5(s):
    """ 获取md5 """
    s = str(s)
    return hashlib.md5(f'{s}'.encode('utf-8')).hexdigest()


def _get_end_point() -> str:
    """获取相应的endpoint"""
    endpoint = END_POINT
    return endpoint


class QueryTypeNotExistError(Exception):
    def __init__(self, msg=None):
        self.msg = msg
    
    def __str__(self):
        s = '查询类型不存在 '
        if self.msg:
            s = s + str(self.msg)
        return s


def item2dict(l):
    """将aliyun table查询中返回的数据变成字典
    示例：
        源数据: ([('primary_key': 12345)], [('col1','value1'), ('col2', 'value2')...])
        转换后的数据:
            {
                'primary_key': 12345,
                'col1': 'value1',
                'col2': 'value2',
                ...
            }  
    """
    res = {}
    # primary key-values and column key-values.
    pkvs, ckvs = l
    # 主键的键值对.
    for pk, pv in pkvs:
        res[pk] = pv
    # 存放最终结果的字典.
    for ck, cv, _ in ckvs:
        res[ck] = cv
    return res


class TableClient(object):
    """
    Realization of some database operations in Aliyun OTSclient.

    """
    def __init__(
                 self,
                 instance_name,
                 end_point=None,
                 access_key_id=None, 
                 access_key_secret=None):
        """
        初始化``OTSClient``实例。
        ``end_point``是表格存储服务的地址（例如 'https://instance.cn-hangzhou.ots.aliyun.com:80'），必须以'https://'开头。
        ``access_key_id``是访问表格存储服务的 AccessKeyID，通过官方网站申请或通过管理员获取。
        ``access_key_secret``是访问表格存储服务的AccessKeySecret，通过官方网站申请或通过管理员获取。
        ``instance_name``是要访问的实例名，通过官方网站控制台创建或通过管理员获取。
    ``sts_token``是访问表格存储服务的STS token，从阿里云STS服务获取，具有有效期，过期后需要重新获取。
        ``encoding``请求参数的字符串编码类型，默认是 utf8。
        ``socket_timeout``是连接池中每个连接的 Socket 超时，单位为秒，可以为 int 或 float。默认值为 50。
        ``max_connection``是连接池的最大连接数。默认为 50。
        ``logger_name``用来在请求中打 DEBUG 日志，或者在出错时打 ERROR 日志。
        ``retry_policy``定义了重试策略，默认的重试策略为 DefaultRetryPolicy。你可以继承 RetryPolicy 来实现自己的重试策略，请参考 DefaultRetryPolicy 的代码。
        """
        # Get endpoint.
        if end_point is None:
            end_point = os.environ.get('OTS_END_POINT')
        if end_point is None:
            raise Exception('OTS_END_POINT not set, you can set it in ENV.')
        # Get access_key_id.
        if access_key_id is None:
            access_key_id = os.environ.get('OTS_ACCESS_KEY_ID')
        if access_key_id is None:
            raise Exception('OTS_ACCESS_KEY_ID not set, you can set it in ENV.')
        # Get access_key_secret
        if access_key_secret is None:
            access_key_secret = os.environ.get('OTS_ACCESS_KEY_SECRET')
        if access_key_secret is None:
            raise Exception('OTS_ACCESS_KEY_SECRET not set, you can set it in ENV.')

        # Link to OTSclient
        self.otsclient = OTSClient(end_point=end_point,
                                   access_key_id=access_key_id,
                                   access_key_secret=access_key_secret,
                                   instance_name=instance_name,
                                   max_connection=300,
                                   socket_timeout=2,
                                   retry_policy = WriteRetryPolicy())


        self.instance_name = instance_name 
        # 保存全部的数据表
        self.table_list = list(self.get_table_list())


    def query_all(self, table_name, primary_key='_id', start_primary_key=None, limit=None):
        yield_data_count = 0
        start_key = start_primary_key if start_primary_key else INF_MIN
        consumed, next_start_primary_key, row_list, next_token = self.otsclient.get_range(
                table_name, 
                'FORWARD', 
                [(primary_key, start_key)], 
                [(primary_key, INF_MAX)], 
                limit=100,
        )
        ind = 0
        for row in row_list:
            row_item = []
            row_item.append(list(row.primary_key))
            row_item.append(list(row.attribute_columns))
            d = item2dict(row_item)
            ind += 1
            yield d
            yield_data_count += 1
            if (limit is not None) and (yield_data_count >= limit):
                return
        while next_start_primary_key:
             consumed, next_start_primary_key, row_list, next_token = self.otsclient.get_range(
                     table_name, 
                     'FORWARD', 
                     [(primary_key, next_start_primary_key[0][1])], 
                     [(primary_key, INF_MAX)], 
                     limit=100,
                     )
             for row in row_list:
                 row_item = []
                 row_item.append(list(row.primary_key))
                 row_item.append(list(row.attribute_columns))
                 d = item2dict(row_item)
                 ind += 1
                 if (limit is not None) and  ind > limit:
                     return
                 yield d

    def _construct_query_object(self, query_type, column_name, query_content):
        """
        根据用户输入的查询条件构造单个简单阿里云查询.
        user_query 为三元组, 
            (query_type, column_name, query_content)
            (查询类型，列名称，查询内容)
        查询条件为字符或者列表(只有terms时用到列表).
        支持的查询方式:
            term: 精准查询
                功能：查询`column_name`列, 值为query_content的数据
                e.g. 
                    user_query = ('term', 'user', '用户9523')
                    query = _construct_query_object(user_query)
            terms: 精准查询
                功能：查询 `column_name`列的值在query_content列表 的数据
                e.g. 
                    user_query = ('term', 'user', '用户9523')
                    query = _construct_query_object(user_query)
            range: 范围查询
                功能：查询 `column_name`列的值在给定范围的数据, 范围由query_content指定,
                      格式为字符串，写成区间的格式, 支持开闭区间
                e.g.
                    # 构造点赞量在(100, 200]区间内的范围查询
                    user_query = ('range', 'like_count', '(100, 200]')
                    query = _construct_query_object(user_query)
            phrase: 短语匹配查询
                功能：查询 `column_name`列的值中出现query_content的数据
                e.g.
                    # 构造标题包含关键词`王者荣耀`的查询
                    user_query = ('phrase', 'title', '王者荣耀')
            matchall: 
                功能：查询全部数据
                    user_query = ('matchall', '', '')
        """

        def handle_bracket(s):
            l = '()[]'.split()
            s = s.strip()
            for i in l:
                s = s.strip(i)
            return '(' + s + ')'

        # 如果是精确查询 
        if query_type in ['term', 'terms'] :
            query = TermQuery(column_name, query_content)
        # 如果是范围查询
        elif query_type == 'range':
            query_content = query_content.strip()
            # 判断上限和下限的开闭区间.
            include_lower = True if query_content.startswith('[') else False
            include_upper = True if query_content.endswith(']') else False
            # 去掉查询中的括号并加上新的括号
            query_content = handle_bracket(query_content)
            lower, upper = eval(query_content)
            # 构造单个查询对象
            query = RangeQuery(column_name, range_from=lower, range_to=upper, include_lower=include_lower, include_upper=include_upper)
        elif query_type == 'phrase':
            query = MatchPhraseQuery(column_name, query_content)
        elif query_type == 'matchall':
            query = MatchAllQuery()
        elif query_type == 'prefix':
            query = PrefixQuery(col_name, query_content)
        else:
            logger.error('查询类型 {} 出错,请输入正确的查询类型'.format(query_type))
            raise QueryTypeNotExistError(query_type)
        return query

    def construct_query_list(self, user_query_list):
        """
        将用户的输入的查询列表构造为阿里云查询对象列表.
        """
        # 存储最终的查询列表
        aliyun_query_list = []
        # 将用户查询构造为阿里云的查询对象
        for user_query in user_query_list:
            # 获取每个查询的查询类型，列名称，查询条件
            query_type, col_name, query_content = user_query
            # 构造查询对象
            query = self._construct_query_object(query_type, col_name, query_content)
            # 查询对象保存到列表中
            aliyun_query_list.append(query)
        return aliyun_query_list


    def query(self, 
              table_name,
              must_query_list=[], 
              must_not_query_list=[], 
              should_query_list=[], 
              get_total_count=False, 
              sort_list=None, 
              index_name='filter', 
              column_to_get=None, 
              limit=None):
        """
        第一个版本的and查询.
        根据用户输入的查询条件构造阿里云查询.
        :param must_query_list: 需要满足的查询条件列表, 不指定则默认为查询全部数据
        :param must_not_query_list: 不查询的条件列表（满足这些条件的数据不查询）
        :param should_query_list: 不查询的条件列表（满足这些条件的数据不查询）
        :param get_total_count: 是否需要获取查询到的总数量
        :param sort_list: 列的排序列表，格式为list. 默认不排序
            e.g.
                # 根据时间正序，如果时间相同就根据点赞数倒序
                table_cli = TableClient(table_name='toutiaoAccount', instance_name='account')
                query_list = [
                    ('range', 'fans_count', '[10000, 100000000000)'),
                    ('range', 'datetime', '[1570778199000, 1571778199111]'),
                ]
                sort_list = [
                    ('datetime', 1),
                    ('like_count', -1),
                ]
                for total in table_cli.query(query_list=query_list, sort_list=sort_list, get_total_count=True):
                    print(total)
                    
        :param index_name:
        :param column_to_get:
        用户指定的查询应为三元组, 
            (query_type, column_name, query_content)
            (查询类型，查询那一列，查询内容)
        查询条件为字符或者列表(只有terms时用到列表).
        """

        # 已返回的数据量.
        yield_data_count = 0
    
        # 将用户查询分别构造为阿里云的查询对象
        must_query_list = self.construct_query_list(must_query_list)
        must_not_query_list = self.construct_query_list(must_not_query_list)
        should_query_list = self.construct_query_list(should_query_list)
        if should_query_list:
            must_query_list.append(BoolQuery(should_queries=should_query_list)) 
        if must_not_query_list:
            must_query_list.append(BoolQuery(must_queries=must_not_query_list)) 

        # 构造布尔查询
        bool_query = BoolQuery(
            must_queries=must_query_list,
        )
        # 构造返回指定的列
        if column_to_get is None:
            column = ColumnsToGet(return_type=ColumnReturnType.ALL)
        else:
            column = ColumnsToGet(column_to_get, ColumnReturnType.SPECIFIED)
        # 构造排序
        sort = None
        if sort_list:
            # 用于保存单个列FieldSort排序对象
            sort_obj_list = []
            # 对用户所有排序构造排序对象
            for sort_col, sort_num in sort_list:
                # 构造升降序
                sort_order = SortOrder.DESC if sort_num < 0 else SortOrder.ASC
                # 构造列排序对象
                sort_obj = FieldSort(sort_col, sort_order)
                # 保存到列表中
                sort_obj_list.append(sort_obj)
            #sort = Sort(sorters=[FieldSort(sort_col, sort_order)])
            sort = Sort(sorters=sort_obj_list)
        # 开始查询
        next_token = None
        search_query = SearchQuery(bool_query, sort=sort, limit=100, get_total_count=True)
        while True:
            rows, next_token, total_count, is_all_succeed = self.otsclient.search(
                            table_name, index_name, search_query, column
            )
            # 返回数据
            for row in rows:
                d = item2dict(row)
                # 根据是否需要总数准备返回数据
                prepared_data = (total_count, d) if get_total_count else d
                yield prepared_data
                yield_data_count += 1
                if (limit is not None) and (yield_data_count >= limit):
                    return
                    
            # 如果没有数据就跳出循环
            if not next_token: # data all returned
                break
            # 后续循环不需要排序了.
            search_query = SearchQuery(bool_query, next_token=next_token, limit=100, get_total_count=True)

    def list_index(self, table_name, index_name):
        """
        List all search indexes, or indexes under one table.
        Example usage:
            index_list = table_cli.list_index('table1', 'filter')
            print(index_list)
        """
        self.otsclient.delete_search_index(table_name, index_name)
        return self._request_helper('ListSearchIndex', table_name)

    def delete_index(self, table_name, index_name):
        """
        Delete the search index.
        Example usage:
            table_cli.list_index('table1', 'index1')
        """
        self.otsclient.delete_search_index(table_name, index_name)

    def show_index(self, table_name, index_name='filter'):
        """
        输出索引信息.
        """
        index_meta, sync_stat = self.otsclient.describe_search_index(table_name, index_name)
        print ('sync stat: %s, %d' % (str(sync_stat.sync_phase), sync_stat.current_sync_timestamp))
        print ('index name: %s' % index_name)
        print ('index fields:')
        header = ['字段名', '字段类型', '是否索引', '是否数组', '允许排序', '附加存储']
        table = PrettyTable(header)
        for field in index_meta.fields:
            l = []
            l.append(field.field_name)
            l.append(str(field.field_type))
            l.append(field.index)
            l.append(field.is_array)
            l.append(field.enable_sort_and_agg)
            l.append(field.store)
            table.add_row(l)
        print(table)

    def create_index(self, table_name, index_name, index_meta):
        """
        Create search index.
        :type table_name: str
        :param table_name: The name of table.
        :type index_name: str
        :param index_name: The name of index.
        :type index_meta: tablestore.metadata.SearchIndexMeta
        :param index_meta: The definition of index, includes fields' schema, index setting and index pre-sorting configuration.
        Example usage:
            field_a = FieldSchema('k', FieldType.KEYWORD, index=True, enable_sort_and_agg=True, store=True)
            field_b = FieldSchema('t', FieldType.TEXT, index=True, store=True, analyzer=AnalyzerType.SINGLEWORD)
            field_c = FieldSchema('g', FieldType.GEOPOINT, index=True, store=True)
            field_d = FieldSchema('ka', FieldType.KEYWORD, index=True, is_array=True, store=True)
            nested_field = FieldSchema('n', FieldType.NESTED, sub_field_schemas=
                [
                    FieldSchema('nk', FieldType.KEYWORD, index=True, enable_sort_and_agg=True, store=True),
                    FieldSchema('nt', FieldType.TEXT, index=True, store=True, analyzer=AnalyzerType.SINGLEWORD),
                    FieldSchema('ng', FieldType.GEOPOINT, index=True, store=True, enable_sort_and_agg=True)
                ])
           fields = [field_a, field_b, field_c, field_d, nested_field]
           index_meta = SearchIndexMeta(fields, index_setting=None, index_sort=None)
           table_client.create_search_index('table_1', 'index_1', index_meta)
        """
        self.otsclient.create_search_index(self, table_name, index_name, index_meta)


    def put_row(self, table_name, pk_list, data):
        """
        写入数据,写入成功返回消耗cu.

        :param pk_list [list]: primary key name list. e.g. ['pk1', 'pk2']
        :param data [dict]: 包括主键在内的数据字典.
        """
        # Deep copy data.
        new_data = deepcopy(data)
        for item in new_data:
            if isinstance(new_data.get(item), dict) or isinstance(new_data.get(item), list):
                new_data[item] = json.dumps(new_data[item])

        # Generate primay key.
        primary_key = []
        for pk_name in pk_list:
            # Detect auto increase column.
            pk_value = new_data[pk_name]
            if pk_value is None:
                pk_value = PK_AUTO_INCR
            pk = (pk_name, pk_value)
            new_data.pop(pk_name)
            primary_key.append(pk)
        # Generate attribute columns.
        attribute_columns = [(key, new_data[key]) for key in new_data]
        # Generate Row object.
        row = Row(primary_key, attribute_columns)

        # 表示只有此行不存在时，才会插入数据，否则不执行(报错)
        #condition = Condition(RowExistenceExpectation.EXPECT_NOT_EXIST)
        # 表示不管此行是否已经存在，都会插入新数据，如果之前有会被覆盖。
        # condition = Condition(RowExistenceExpectation.IGNORE)

        # 插入数据
        try:
            #cu, _ = self.otsclient.put_row(table_name, row, condition)
            cu, _ = self.otsclient.put_row(table_name, row)
            return cu.write
        except Exception as e:
            logger.error(e)
            #pass

    def update_row(self, table_name, pk_list, data):
        """
        Update row

        :param pk_list [list]: primary key name list. e.g. ['pk1', 'pk2']
        :param data [dict]: 包括主键在内的数据字典.
        """
        # Deep copy data.
        new_data = deepcopy(data)
        for item in new_data:
            if isinstance(new_data.get(item), dict) or isinstance(new_data.get(item), list):
                new_data[item] = json.dumps(new_data[item])

        # Generate primay key.
        primary_key = []
        for pk_name in pk_list:
            # Detect auto increase column.
            pk_value = new_data[pk_name]
            if pk_value is None:
                pk_value = PK_AUTO_INCR
            pk = (pk_name, pk_value)
            new_data.pop(pk_name)
            primary_key.append(pk)
        # Generate attribute columns.
        attribute_columns = [(key, new_data[key]) for key in new_data]
        # Generate Row object.
        row = Row(primary_key, {'PUT':attribute_columns})

        condition = Condition(RowExistenceExpectation.IGNORE)
        try:
            consumed, return_row = self.otsclient.update_row(table_name, row, condition)
            return consumed.write, return_row
        # 客户端异常，一般为参数错误或者网络异常。
        except OTSClientError as e:
            logger.error(f'Client error, {e}')
        # 服务端异常，一般为参数错误或者流控错误。
        except OTSServiceError as e:
            logger.error(f'Server error, {e}')

    def get_table_list(self):
        """
        Get table name list.
        :return: tuple of table name
        """
        return self.otsclient.list_table()


class Test():
    def test_put_row(self):
        table_cli = TableClient(instance_name='nm-sea')
        d = {'medium_id': 2222222, 'id':None, 'content':'test article '}
        l = table_cli.put_row('all_news', ['medium_id', 'id'], d)
        print(l)

    def test_query(self):
        table_cli = TableClient(instance_name='nm-sea')
        must_query = [
            ('term', 'medium_id', 2222222),
        ]
        a = table_cli.query(table_name='all_news',
                            must_query_list=must_query, 
                            get_total_count=False, 
                            index_name='test',)
        for i in a:
            return i.get('id')


    def test_update_row(self):
        table_cli = TableClient(instance_name='nm-sea')
        _id = self.test_query()
        d = {'medium_id': 2222222, 'id':_id, 'content':'222222 article '}
        l = table_cli.update_row('all_news', ['medium_id', 'id'], d)
        print(l)

    def test_delete_index(self):
        table_cli = TableClient(instance_name='nm-sea')
        table_cli.otsclient.delete_search_index('all_news', 'test')




