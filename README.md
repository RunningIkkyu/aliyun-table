# aliyun-table

阿里云表格相关操作

目前支持：

[ ] 输出索引信息
[ ] 查询全部数据
[ ] 表格查询：短语查询，前缀匹配查询，精准查询，范围查询
[x] 插入数据
[x] 更新数据


# CHANGELOG



## 安装

```bash
pip install --upgrade aliyun-table
```


安装完成后，在交互界面执行：


```python
>>> from aliyun_table import TableClient
>>>
```

如果没有报错，说明安装成功


### 初始化

以下变量可以设置为环境变量

```OTS_END_POINT```

```OTS_ACCESS_KEY_ID```

```OTS_ACCESS_KEY_SECRET```


```python
from aliyun_table import TableClient
table_cli = TableClient()
```



### 插入数据和更新数据

```python

table_cli = TableClient(table_name='表名', 
                        instance_name='实例名',
                        end_point='endpoint',
                        access_key_id='access_key_secret', 
                        access_key_secret='access_key_secret'):
data = {
    'pk1': '123',
    # pk2为自增列，需要传None.
    'pk2': None,
    'col1': 'test'
    'article': 'This is a test article.'
}
cu = table_cli.put_row(table_name='表名', pk_list=['pk1', 'pk2'], data=data)
print(cu)
```

### Reference

```python

class TableClient(object):
    def __init__(
                 self,
                 table_name,
                 instance_name,
                 end_point=None,
                 access_key_id=None, 
                 access_key_secret=None):
        ...

    def show_index(self, index_name='filter'):
        ...

    def put_row(self, table_name, pk_list, data):
        """
        写入数据,写入成功返回消耗cu.

        :param pk_list [list]: primary key name list. e.g. ['pk1', 'pk2']
        :param data [dict]: 包括主键在内的数据字典.
        """
        ... 

    def update_row(self, pk_list, data):
        """
        Update row

        :param pk_list [list]: primary key name list. e.g. ['pk1', 'pk2']
        :param data [dict]: 包括主键在内的数据字典.
        """
        ...

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
        :param must_query_list [list]: 需要满足的查询条件列表, 不指定则默认为查询全部数据
        :param must_not_query_list [list]: 不查询的条件列表（满足这些条件的数据不查询）
        :param should_query_list [list]: 不查询的条件列表（满足这些条件的数据不查询）
        :param get_total_count [bool]: 是否需要获取查询到的总数量
        :param sort_list [list]: 列的排序列表，格式为list. 默认不排序
        :param limit [int]: 最多返回多少数量的数据
        :return: 查询到数据的迭代器，每个数据根据get_total_count的取值有所不同
        ...
```
