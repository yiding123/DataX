### Datax MongoDBSimpleReader
#### 1 快速介绍

MongoDBSimpleReader 是基于MongoDBReader做了简化处理，可以用于全字段copy，无需配置column的映射。Mongodb是文档结构，document格式灵活，同一collection内doc格式并不固定，所以如果在配置字段时比较麻烦，对于全部copy的场景其实我们并不需要关心具体字段，此插件就是为了解决非固定格式的document下全字段同步的。
MongoDBSimpleReader 插件利用 MongoDB 的java客户端MongoClient进行MongoDB的读操作。最新版本的Mongo已经将DB锁的粒度从DB级别降低到document级别，配合上MongoDB强大的索引功能，基本可以达到高性能的读取MongoDB的需求。

#### 2 实现原理

MongoDBSimpleReader 通过Datax框架从MongoDB并行的读取数据，通过主控的JOB程序按照指定的规则对MongoDB中的数据进行分片，并行读取，然后将MongoDB支持的类型通过逐一判断转换成Datax支持的类型。

#### 3 功能说明
* 该示例从ODPS读一份数据到MongoDB。

	    {
	    "job": {
	        "setting": {
	            "speed": {
	                "channel": 2
	            }
	        },
	        "content": [
	            {
	                "reader": {
	                    "name": "mongodbreader",
	                    "parameter": {
	                        "address": ["127.0.0.1:27017"],
	                        "userName": "",
	                        "userPassword": "",
	                        "dbName": "tag_per_data",
	                        "collectionName": "tag_data12",
	                        "column": []
	                    }
	                },
	                "writer": {
	                    "name": "odpswriter",
	                    "parameter": {
	                        "project": "tb_ai_recommendation",
	                        "table": "jianying_tag_datax_read_test01",
	                        "column": [],
	                        "accessId": "**************",
	                        "accessKey": "********************",
	                        "truncate": true,
	                        "odpsServer": "xxx/api",
	                        "tunnelServer": "xxx",
	                        "accountType": "aliyun"
	                    }
	                }
	            }
	        ]
	    }
        }
#### 4 参数说明

* address： MongoDB的数据地址信息，因为MonogDB可能是个集群，则ip端口信息需要以Json数组的形式给出。【必填】
* userName：MongoDB的用户名。【选填】
* userPassword： MongoDB的密码。【选填】
* collectionName： MonogoDB的集合名。【必填】
* column：无需配置,同构(mongodb),全部同步
* query: MongoDB的额外查询条件。【选填】

#### 5 类型转换

| DataX 内部类型| MongoDB 数据类型    |
| -------- | -----  |
| Long     | int, Long |
| Double   | double |
| String   | string, array |
| Date     | date  |
| Boolean  | boolean |
| Bytes    | bytes |


#### 6 性能报告
#### 7 测试报告