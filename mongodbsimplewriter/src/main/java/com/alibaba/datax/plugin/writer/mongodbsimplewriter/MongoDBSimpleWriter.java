package com.alibaba.datax.plugin.writer.mongodbsimplewriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.SimpleColumn;
import com.alibaba.datax.common.element.SimpleRecord;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.datax.plugin.writer.mongodbsimplewriter.util.MongoUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MongoDBSimpleWriter extends Writer{

    public static class Job extends Writer.Job {

        private Configuration originalConfig = null;

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configList = new ArrayList<Configuration>();
            for(int i = 0; i < mandatoryNumber; i++) {
                configList.add(this.originalConfig.clone());
            }
            return configList;
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
        }

        @Override
        public void prepare() {
            super.prepare();
        }

        @Override
        public void destroy() {

        }
    }

    public static class Task extends Writer.Task {

        private static final Logger logger = LoggerFactory.getLogger(Task.class);
        private   Configuration       writerSliceConfig;

        private MongoClient mongoClient;
        private String database = null;
        private String collection = null;
        private Integer batchSize = null;
        private JSONObject writeMode = null;
        private static int BATCH_SIZE = 1000;
        private String userName = null;
        private String password = null;

        @Override
        public void prepare() {
            super.prepare();
            //获取presql配置，并执行
            String preSql = writerSliceConfig.getString(Key.PRE_SQL);
            if(Strings.isNullOrEmpty(preSql)) {
                return;
            }
            Configuration conConf = Configuration.from(preSql);
            if(Strings.isNullOrEmpty(database) || Strings.isNullOrEmpty(collection)
                    || mongoClient == null || batchSize == null) {
                throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,
                        MongoDBWriterErrorCode.ILLEGAL_VALUE.getDescription());
            }
            MongoDatabase db = mongoClient.getDatabase(database);
            MongoCollection col = db.getCollection(this.collection);
            String type = conConf.getString("type");
            if (Strings.isNullOrEmpty(type)){
                return;
            }
            if (type.equals("drop")){
                col.drop();
            } else if (type.equals("remove")){
                String json = conConf.getString("json");
                BasicDBObject query;
                if (Strings.isNullOrEmpty(json)) {
                    query = new BasicDBObject();
                    List<Object> items = conConf.getList("item", Object.class);
                    for (Object con : items) {
                        Configuration _conf = Configuration.from(con.toString());
                        if (Strings.isNullOrEmpty(_conf.getString("condition"))) {
                            query.put(_conf.getString("name"), _conf.get("value"));
                        } else {
                            query.put(_conf.getString("name"),
                                    new BasicDBObject(_conf.getString("condition"), _conf.get("value")));
                        }
                    }
//              and  { "pv" : { "$gt" : 200 , "$lt" : 3000} , "pid" : { "$ne" : "xxx"}}
//              or  { "$or" : [ { "age" : { "$gt" : 27}} , { "age" : { "$lt" : 15}}]}
                } else {
                    query = (BasicDBObject) com.mongodb.util.JSON.parse(json);
                }
                col.deleteMany(query);
            }
            if(logger.isDebugEnabled()) {
                logger.debug("After job prepare(), originalConfig now is:[\n{}\n]", writerSliceConfig.toJSON());
            }
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            if(Strings.isNullOrEmpty(database) || Strings.isNullOrEmpty(collection)
                    || mongoClient == null || batchSize == null) {
                throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,
                                                MongoDBWriterErrorCode.ILLEGAL_VALUE.getDescription());
            }
            MongoDatabase db = mongoClient.getDatabase(database);
            MongoCollection<Document> col = db.getCollection(this.collection);
            List<Record> writerBuffer = new ArrayList<Record>(this.batchSize);
            Record record = null;
            while((record = lineReceiver.getFromReader()) != null) {
                writerBuffer.add(record);
                if(writerBuffer.size() >= this.batchSize) {
                    doBatchInsert(col,writerBuffer);
                    writerBuffer.clear();
                }
            }
            if(!writerBuffer.isEmpty()) {
                doBatchInsert(col,writerBuffer);
                writerBuffer.clear();
            }
        }

        private void doBatchInsert(MongoCollection<Document> collection, List<Record> writerBuffer) {
            List<Document> dataList = new ArrayList<Document>();
            List<SimpleColumn> docOrginal = null;
            Document docCopy = null;
            for(Record record : writerBuffer) {
                docOrginal = ((SimpleRecord)record).getColumns(); //不能强制转化成Document，因为被不同的classloader加载，会报 ClassCastException: org.bson.Document cannot be cast to org.bson.Document
                docCopy = new Document();
                for(SimpleColumn column : docOrginal){
                    String key = column.getName();
                    Object value = column.getRawData();
                    logger.debug("key={},value={},type={}",key,value,value.getClass().getName());
                    if("org.bson.types.ObjectId".equals(value.getClass().getName())){
                        docCopy.put(key,value.toString());
                    }else{
                        docCopy.put(key,value);
                    }
                }
                dataList.add(docCopy);
            }
            /**
             * 如果存在重复的值覆盖
             */
            if(this.writeMode != null &&
                    this.writeMode.getString(KeyConstant.IS_REPLACE) != null &&
                    KeyConstant.isValueTrue(this.writeMode.getString(KeyConstant.IS_REPLACE))) {
                String uniqueKey = this.writeMode.getString(KeyConstant.UNIQUE_KEY);
                if(!Strings.isNullOrEmpty(uniqueKey)) {
                    List<ReplaceOneModel<Document>> replaceOneModelList = new ArrayList<ReplaceOneModel<Document>>();
                    for(Document data : dataList) {
                        BasicDBObject query = new BasicDBObject();
                        if(uniqueKey != null) {
                            query.put(uniqueKey,data.get(uniqueKey));
                        }
                        ReplaceOneModel<Document> replaceOneModel = new ReplaceOneModel<Document>(query, data, new UpdateOptions().upsert(true));
                        replaceOneModelList.add(replaceOneModel);
                    }
                    collection.bulkWrite(replaceOneModelList, new BulkWriteOptions().ordered(false));
                } else {
                    throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,
                            MongoDBWriterErrorCode.ILLEGAL_VALUE.getDescription());
                }
            } else {
                collection.insertMany(dataList);
                dataList.clear();
            }
        }


        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.database = writerSliceConfig.getString(KeyConstant.MONGO_DB_NAME);
            this.userName = writerSliceConfig.getString(KeyConstant.MONGO_USER_NAME);
            this.password = writerSliceConfig.getString(KeyConstant.MONGO_USER_PASSWORD);
            if(!Strings.isNullOrEmpty(userName) && !Strings.isNullOrEmpty(password)) {
                this.mongoClient = MongoUtil.initCredentialMongoClient(this.writerSliceConfig,userName,password,database);
            } else {
                this.mongoClient = MongoUtil.initMongoClient(this.writerSliceConfig);
            }
            this.collection = writerSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);
            this.batchSize = BATCH_SIZE;
            //this.mongodbColumnMeta = JSON.parseArray(writerSliceConfig.getString(KeyConstant.MONGO_COLUMN));
            this.writeMode = JSON.parseObject(writerSliceConfig.getString(KeyConstant.WRITE_MODE));
        }

        @Override
        public void destroy() {
            mongoClient.close();
        }
    }

}
