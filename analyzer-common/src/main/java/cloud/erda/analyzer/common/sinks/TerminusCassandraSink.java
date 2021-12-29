// Copyright (c) 2021 Terminus, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cloud.erda.analyzer.common.sinks;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import cloud.erda.analyzer.common.utils.CassandraBaseCommitter;
import cloud.erda.analyzer.common.utils.CassandraSinkHelper;
import cloud.erda.analyzer.common.utils.CassandraSinkUtils;
import cloud.erda.analyzer.common.utils.StringUtil;
import com.google.common.base.Throwables;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.connectors.cassandra.CassandraPojoSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

/**
 * example1: 自动建表 POJO
 * TerminusCassandraSink.addSink(model)
 * .enableWriteAheadLog()
 * .setCreateTable(true)
 * .setCreateKeySpace(true)
 * .setBatchSize(100)
 * .setHosts("localhost")
 * .setKeyspace("test1")
 * .build();
 * <p>
 * example2: 非自动建表 POJO
 * TerminusCassandraSink.addSink(model)
 * .setAsync(false)
 * .setBatchSize(100)
 * .setHosts("localhost")
 * .setKeyspace("test1")
 * .build();
 * <p>
 * example3: 非POJO,use columns
 * TerminusCassandraSink.addSink(model)
 * .setTableName("test2")
 * .setKeyspace("test1")
 * .setHosts("localhost")
 * .setColumns("name,ip,time,longtest")
 * .setColumnsValueFunction((Function<TestModel, List<Object>> & Serializable) event -> {
 * return Lists.newArrayList(event.getName(), event.getTags().getOrDefault("host_ip", "")
 * , event.getTime(), event.getLongtest());
 * }).build();
 * //CREATE TABLE test1.test2 (name text,ip text,time timestamp,longtest bigint,PRIMARY KEY (name, ip, time))
 * <p>
 * examle4: 非POJO,use insert sql
 * TerminusCassandraSink.addSink(model)
 * .setHosts("localhost")
 * .setExecuteSql(" insert into test1.test2(name,ip,time,longtest) values(?,?,?,?)")
 * .setColumnsValueFunction((Function<TestModel, List<Object>> & Serializable) event -> {
 * return Lists.newArrayList(event.getName(), event.getTags().getOrDefault("host_ip", "")
 * , event.getTime(), event.getLongtest());
 * }).build();
 * <p>
 * example5:  setCreateKeySpace,setCreateTable的位置需要在table配置完，host配置完之后再进行
 * TerminusCassandraSink.addSink(model)
 * .enableWriteAheadLog()
 * .setAsync(false)
 * .setHosts("localhost")
 * .setKeyspace("test1")
 * .addPreSql("drop table test1.test14;")
 * .addTableCacheConfig("keys", "NONE")
 * .addTableCompressionConfig("chunk_length_in_kb", "128")
 * .addTableComactionConfig("max_threshold","128")
 * .setCreateKeySpace(true)
 * .setCreateTable(true)
 * .setBatchSize(100)
 * .setKeyspace("test1")
 * .build();
 * TestModel:
 *
 * @Data
 * @Table(keyspace = "test1", name = "test14")
 * @TableConfig(comment = "test", defaultTimeToLive = 100)
 * public class TestModel {
 * @PartitionKey
 * @Column(name = "name111")
 * private String name;
 * @ClusteringColumn
 * @Column(name = "ip1111")
 * private String ip;
 * @ClusteringColumn(1)
 * @ClusteringOrder(DESC) // DESC,ASC
 * @Column(name = "time1111")
 * private Long time;
 * @ClusteringColumn(2)
 * @Column(name = "tags")
 * private Map<String, String> tags;
 * @Column(name = "longtest1111")
 * private Long longtest;
 * <p>
 * public static TestModel map(MetricEvent event) {
 * TestModel model = new TestModel();
 * model.setName(event.getName());
 * model.setIp(event.getTags().getOrDefault("host_ip", ""));
 * model.setTime((event.getTimestamp() / 1000000));
 * model.setLongtest((event.getTimestamp() / 1000000));
 * model.setTags(event.getTags());
 * return model;
 * }
 * }
 **/

@Slf4j
public class TerminusCassandraSink<T> {

    public static <T> CassandraBaseSinkBuilder<T> addSink(DataStream<T> input) {
        return builder(input);
    }

    protected static <T> CassandraBaseSinkBuilder builder(DataStream<T> input) {
        CassandraBaseSinkBuilder builder = new CassandraBaseSinkBuilder();
        builder.setSource(input);
        return builder;
    }

    public static class CassandraBaseSinkBuilder<T> implements Serializable {

        private DataStream<T> source;

        private CassandraSinkParam param = new CassandraSinkParam();

        public CassandraBaseSinkBuilder() {
        }

        public CassandraBaseSinkBuilder enableWriteAheadLog() {
            this.param.withCommitter = true;
            return this;
        }

        public CassandraBaseSinkBuilder setSource(DataStream<T> source) {
            this.source = source;
            this.setExecutionConfig(source.getExecutionConfig());
            this.param.setCheckpointConfig(source.getExecutionEnvironment().getCheckpointConfig());
            this.setTypeInfo(source.getType());
            return this;
        }

        public CassandraBaseSinkBuilder setReplicationFactor(int replicationFactor) {
            this.param.replicationFactor = replicationFactor;
            return this;
        }

        public CassandraBaseSinkBuilder setKeySpaceClass(String keySpaceClass) {
            this.param.keySpaceClass = keySpaceClass;
            return this;
        }

        private CassandraBaseSinkBuilder setTypeInfo(TypeInformation info) {
            this.param.typeInfo = info;
            return this;
        }

        public CassandraBaseSinkBuilder setParallelism(int parallelism) {
            this.param.parallelism = parallelism;
            return this;
        }

        public CassandraBaseSinkBuilder setHosts(String hosts) {
            this.param.hosts = hosts;
//            if (this.param.clusterBuilder == null) {
//                this.param.clusterBuilder = new CassandraClusterBuilder(hosts);
//            }
            return this;
        }

        public CassandraBaseSinkBuilder setUserName(String userName) {
            this.param.setUserName(userName);
            return this;
        }

        public CassandraBaseSinkBuilder setPassword(String password) {
            this.param.setPassword(password);
            return this;
        }

        public CassandraBaseSinkBuilder useCredentials(boolean useCredentials) {
            this.param.setUseCredentials(useCredentials);
            return this;
        }

        public CassandraBaseSinkBuilder setClusterBuilder(ClusterBuilder builder) {
            this.param.clusterBuilder = builder;
            return this;
        }

        public CassandraBaseSinkBuilder setKeyspace(String keyspace) {
            this.param.keySpace = keyspace;
            return this;
        }

        public CassandraBaseSinkBuilder setTtl(int ttl) {
            this.param.ttl = ttl;
            return this;
        }

        public CassandraBaseSinkBuilder setBatchSize(int batchSize) {
            this.param.batchSize = batchSize;
            return this;
        }

        public CassandraBaseSinkBuilder setInterval(int interval) {
            this.param.interval = interval;
            return this;
        }

        public CassandraBaseSinkBuilder setAsync(boolean async) {
            this.param.async = async;
            return this;
        }

        public CassandraBaseSinkBuilder setCreateTable(boolean createTable) {
            this.param.createTable = createTable;
            return this;
        }

        public CassandraBaseSinkBuilder setCreateKeySpace(boolean createKeySpace) {
            this.param.createKeySpace = createKeySpace;
            return this;
        }

        public CassandraBaseSinkBuilder addTableCacheConfig(String key, String value) {
            this.param.tableCacheConfig.put(key, value);
            return this;
        }

        public CassandraBaseSinkBuilder addTableComactionConfig(String key, String value) {
            this.param.tableCompactionConfig.put(key, value);
            return this;
        }

        public CassandraBaseSinkBuilder setTableGcGraceSecondsConfig(int seconds) {
            this.param.tableGcGraceSecondsConfig = seconds;
            return this;
        }

        public CassandraBaseSinkBuilder addTableCompressionConfig(String key, String value) {
            this.param.tableCompressionConfig.put(key, value);
            return this;
        }


        public CassandraBaseSinkBuilder setTableName(String tableName) {
            this.param.tableName = tableName;
            return this;
        }

        public CassandraBaseSinkBuilder setColumns(String columns) {
            this.param.columns = columns;
            return this;
        }

        /*
         *example:(Function<MetricEvent, List<Object>> & Serializable) event -> return new ArrayList<Object>();
         */
        public CassandraBaseSinkBuilder setColumnsValueFunction(Function<T, List<Object>> function) {
            this.param.columnValuesFunction = function;
            return this;
        }

        public CassandraBaseSinkBuilder addPreSql(String sql) {
            if (sql != null && !sql.isEmpty()) {
                this.param.preSQLs.add(sql);
            }
            return this;
        }

        public CassandraBaseSinkBuilder setExecuteSql(String sql) {
            if (sql != null && !sql.isEmpty()) {
                this.param.executeSQL = sql;
            }
            return this;
        }

        private CassandraBaseSinkBuilder setExecutionConfig(ExecutionConfig executionConfig) {
            this.param.config = executionConfig;
            return this;
        }

        public void build() throws Exception {
            if (this.param.clusterBuilder == null) {
                this.param.clusterBuilder = new CassandraClusterBuilder(this.param.getHosts(), this.param.isUseCredentials(), this.param.getUserName(), this.param.getPassword());
            }
            int parallelism = param.getParallelism();
            if (parallelism == 0) {
                parallelism = source.getExecutionEnvironment().getParallelism();
            }

            // 初始化keySpace SQL
            try {
                this.addPreSql(buildCreateKeySpaceSql(param));
            } catch (Exception e) {
                log.error("build create keyspace sql error {}", Throwables.getStackTraceAsString(e));
            }

            // 初始化创建table SQL
            try {
                this.addPreSql(buildCreateTableSql(param));
            } catch (Exception e) {
                log.error("build create table sql error, error {}", Throwables.getStackTraceAsString(e));
            }

            if (param.withCommitter && param.getCheckpointConfig().isCheckpointingEnabled()) {
                executePreSql();
                CassandraBaseWriteAheadSink committer = cassandraBaseSinkWithCommiter();
                source.transform("sink", null, committer).setParallelism(parallelism);
            } else {
                executePreSql();
                source.addSink(createCassandraBaseSink()).name("cassandra sink").setParallelism(parallelism);
            }
        }

        private void executePreSql() {
            Cluster cluster = param.getClusterBuilder().getCluster();
            Session session = cluster.connect();
            List<String> sqls = param.getPreSQLs();
            for (String sql : sqls) {
                try {
                    session.execute(sql);
                    Thread.sleep(500);//wait 500ms for 同步信息
                } catch (Exception e) {
                    log.error("execute sql error {},{}", sql, e);
                    throw new RuntimeException("execute  error, sql:" + sql, e);
                }
                log.info("execute sql success {}", sql);
            }


            closeSessionCluster(session, cluster);

        }


        public CassandraBaseSink createCassandraBaseSink() throws Exception {
            if (param.getTypeInfo() == null) {
                throw new IllegalArgumentException("typeInfo cannot be null");
            }
            return new CassandraBaseSink<T>(param);
        }


        public CassandraBaseWriteAheadSink cassandraBaseSinkWithCommiter() throws Exception {
            CassandraBaseCommitter cassandraBaseCommitter = new CassandraBaseCommitter(param.clusterBuilder);
            cassandraBaseCommitter.setPreSqls(param.preSQLs);
            TypeSerializer typeSerializer = param.typeInfo.createSerializer(param.config);


            return new TerminusCassandraSink.CassandraBaseWriteAheadSink<T>(cassandraBaseCommitter, typeSerializer, param);
        }

        private static String buildCreateTableSql(CassandraSinkParam param) throws Exception {

            if (param.createTable) {
                if (!(param.typeInfo instanceof PojoTypeInfo)) {
                    throw new IllegalArgumentException("only pojo type can create table");
                }
                if (param.hosts == null) {
                    throw new IllegalArgumentException("hosts should be set before set createTable");

                }
                Cluster cluster = param.getClusterBuilder().getCluster();
                Session session = cluster.connect("system");
                MappingManager mappingManager = new MappingManager(session);
                return CassandraSinkUtils.buildCreateTableSql(param, mappingManager, param.typeInfo.getTypeClass());
            }
            return "";
        }

        private static String buildCreateKeySpaceSql(CassandraSinkParam param) throws Exception {
            if (param.createKeySpace) {
                if (param.keySpace == null) {
                    throw new IllegalArgumentException("keyspace is null");
                }
                if (param.hosts == null) {
                    throw new IllegalArgumentException("hosts should be set before set createTable");

                }
                return CassandraSinkUtils.buildCreateKeySpaceSql(param);
            }
            return "";
        }

    }

    protected static class CassandraBaseSink<T> extends CassandraPojoSink<T> {
        private final Logger logger = LoggerFactory.getLogger(CassandraBaseSink.class);
        private transient Cluster cluster;
        private transient PreparedStatement preparedStatement;
        private CassandraSinkParam<T> param;
        private List<T> queue;
        private long lastExecTimestamp;

        public CassandraBaseSink(CassandraSinkParam param) {
            super(param.typeInfo.getTypeClass(), param.clusterBuilder);
            this.param = param;
            sanityCheck();
            queue = new ArrayList<>(param.batchSize);
        }


        protected void sanityCheck() {
            if (!(param.typeInfo instanceof PojoTypeInfo)) {
                if (param.columnValuesFunction == null) {
                    throw new IllegalArgumentException("columnValuesFunction cannot be null");
                } else if (param.columns == null && StringUtils.isEmpty(param.executeSQL)) {
                    throw new IllegalArgumentException("columns or executSql cannot be null");
                }
            }
        }

        @Override
        public void invoke(T value, Context context) {
            this.queue.add(value);
            if (this.queue.size() >= param.batchSize
                    || System.currentTimeMillis() - this.lastExecTimestamp > param.interval) {
                execute();
                this.lastExecTimestamp = System.currentTimeMillis();
            }
        }

        private void execute() {
            List<Statement> statements = new ArrayList<>(queue.size());
            for (T event : queue) {
                Statement statement = this.buildStatement(event);
                if(statement == null) continue;
                statement.setDefaultTimestamp(System.currentTimeMillis());
                statements.add(statement);
            }
            CassandraSinkHelper.execute(session, statements, param.async, param.batchSize);
            queue = new ArrayList<>(param.batchSize);
        }


        private Statement buildStatement(T event) {

            if (StringUtils.isEmpty(param.executeSQL) && param.columnValuesFunction == null
                    && param.columns == null && param.typeInfo != null) {
                try {
                    return mapper.saveQuery(event);
                } catch (Exception e) {
                    log.error("error,event {},error {}", event, Throwables.getStackTraceAsString(e));
                }
                return null;
            } else {
                List<Object> values = param.columnValuesFunction.apply(event);
                return new BoundStatement(preparedStatement).bind(values.toArray());
            }
        }

        @Override
        public void open(Configuration parameters) {
            this.cluster = this.param.clusterBuilder.getCluster();
            this.session = this.cluster.connect(this.param.keySpace);
            this.queue = new ArrayList<>(this.param.batchSize);
            if (!StringUtils.isEmpty(param.executeSQL)) {
                this.preparedStatement = this.session.prepare(param.executeSQL);

            } else if (param.columns != null && param.columnValuesFunction != null) {
                this.preparedStatement = buildPreparedStatment(session, param);
            }
            if (param.typeInfo instanceof PojoTypeInfo) {
                super.open(parameters);
                this.mappingManager = new MappingManager(session);
                this.mapper = mappingManager.mapper(param.typeInfo.getTypeClass());
                Mapper.Option[] optionsArray = new Mapper.Option[]{
                        Mapper.Option.saveNullFields(true)
                };
                if (param.ttl > 0) {
                    optionsArray = new Mapper.Option[]{
                            Mapper.Option.ttl(param.ttl),
                            Mapper.Option.saveNullFields(true)
                    };
                }
                this.mapper.setDefaultSaveOptions(optionsArray);
            }

        }

        @Override
        public void close() {
            closeSessionCluster(session, cluster);
        }
    }


    protected static class CassandraBaseWriteAheadSink<T> extends GenericWriteAheadSink<T> {
        private final Logger logger = LoggerFactory.getLogger(CassandraBaseWriteAheadSink.class);
        protected transient Cluster cluster;
        protected transient Session session;
        protected transient Mapper<T> mapper;
        protected transient MappingManager mappingManager;
        protected transient PreparedStatement preparedStatement;
        protected CassandraSinkParam<T> param;

        public CassandraBaseWriteAheadSink(CheckpointCommitter committer, TypeSerializer<T> serializer
                , CassandraSinkParam param) throws Exception {
            super(committer, serializer, UUID.randomUUID().toString().replace("-", "_"));
            this.param = param;
            sanityCheck();
        }

        protected void sanityCheck() {
            if (!StringUtils.isEmpty(param.executeSQL) && param.columnValuesFunction == null) {
                throw new IllegalArgumentException("columnValuesFunction cannot be null");
            }
            if (param.config == null) {
                throw new IllegalArgumentException("config cannot be null");
            }
            if (param.typeInfo == null) {
                throw new IllegalArgumentException("typeInfo cannot be null");
            }
            if (!param.getCheckpointConfig().isCheckpointingEnabled()) {
                throw new IllegalArgumentException("checkpoint must enable");
            }
            if (!(param.typeInfo instanceof PojoTypeInfo)
                    && param.columnValuesFunction == null
                    && param.columns == null && param.executeSQL == null) {
                throw new IllegalArgumentException("columns and columnValuesFunction  cannot be null");
            }
        }

        private Statement buildStatement(T event) {
            if ((!StringUtils.isEmpty(param.executeSQL) && param.columnValuesFunction != null)
                    || ((param.columns != null && param.columnValuesFunction != null))) {
                List<Object> values = param.columnValuesFunction.apply(event);
                return new BoundStatement(preparedStatement).bind(values.toArray());
            } else {
                try {
                    return mapper.saveQuery(event);
                } catch (Exception e) {
                    log.error("error,event {},error {}", event, Throwables.getStackTraceAsString(e));
                }
                return null;
            }
        }

        @Override
        public void open() throws Exception {
            super.open();
            this.cluster = this.param.clusterBuilder.getCluster();
            this.session = this.cluster.connect(this.param.keySpace);
            if (!StringUtils.isEmpty(param.executeSQL) && param.columnValuesFunction != null) {
                logger.info("sql:{}", param.executeSQL);
                this.preparedStatement = this.session.prepare(param.executeSQL);

            } else if (param.columns != null && param.columnValuesFunction != null) {
                this.preparedStatement = buildPreparedStatment(session, param);
            }

            if (param.typeInfo != null) {
                try {
                    this.mappingManager = new MappingManager(session);
                    this.mapper = mappingManager.mapper(param.typeInfo.getTypeClass());
                    Mapper.Option[] optionsArray = new Mapper.Option[]{
                            Mapper.Option.saveNullFields(true)
                    };
                    if (param.ttl > 0) {
                        optionsArray = new Mapper.Option[]{
                                Mapper.Option.ttl(param.ttl),
                                Mapper.Option.saveNullFields(true)
                        };
                    }
                    this.mapper.setDefaultSaveOptions(optionsArray);

                } catch (Exception e) {
                    throw new RuntimeException("Cannot create CassandraPojoSink with input: " + param.typeInfo.getTypeClass().getSimpleName(), e);
                }
            }
        }

        @Override
        public void close() {
            closeSessionCluster(session, cluster);
        }

        @Override
        protected boolean sendValues(Iterable<T> values, long checkpointId, long timestamp) throws Exception {
            List<Statement> statements = new ArrayList<>();
            for (T event : values) {
                Statement statement = this.buildStatement(event);
                if (statement != null) {
                    statement.setDefaultTimestamp(System.currentTimeMillis());
                    statements.add(statement);
                }
            }
            return CassandraSinkHelper.execute(session, statements, param.async, param.batchSize);
        }


    }

    private static void closeSessionCluster(Session session, Cluster cluster) {
        try {
            if (session != null) {
                session.close();
            }
            if (cluster != null) {
                cluster.close();
            }
        } catch (Exception e) {
            log.error("Error while close.", e);
        }
    }

    private static <T> PreparedStatement buildPreparedStatment(Session session, CassandraSinkParam<T> param) {
        String[] columns = param.columns.split(",");
        String cql = QueryBuilder.insertInto(param.tableName).values(columns, columns)
                .using(QueryBuilder.ttl(param.ttl)).getQueryString();
        log.info("sql:{}", cql);
        return session.prepare(cql);
    }

    @Data
    public static class CassandraSinkParam<IN> implements Serializable {
        private static final HashMap<String, String> TABLE_CACHE_DEFAULT = new HashMap<String, String>();
        private static final HashMap<String, String> TABLE_COMPACTION_DEFAULT = new HashMap<String, String>();
        private static final HashMap<String, String> TABLE_COMPRESSION_DEFAULT = new HashMap<String, String>();
        private static final int Table_Gc_Grace_Seconds_DEFAULT = 86400;

        static {

            TABLE_CACHE_DEFAULT.put("keys", "ALL");
            TABLE_CACHE_DEFAULT.put("rows_per_partition", "NONE");

            TABLE_COMPACTION_DEFAULT.put("class", "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy");
            TABLE_COMPACTION_DEFAULT.put("max_threshold", "32");
            TABLE_COMPACTION_DEFAULT.put("min_threshold", "4");

            TABLE_COMPRESSION_DEFAULT.put("chunk_length_in_kb", "64");
            TABLE_COMPRESSION_DEFAULT.put("class", "org.apache.cassandra.io.compress.LZ4Compressor");

        }

        private long interval = 5000;
        private ClusterBuilder clusterBuilder;
        private String hosts;
        private int parallelism;
        private String keySpace;
        private CheckpointConfig checkpointConfig;
        private int ttl = 0;
        private int batchSize = 200;
        private boolean async = false;
        private boolean withCommitter = false;
        private TypeInformation<IN> typeInfo;
        private String tableName;
        private String columns;
        private String executeSQL;
        private Function<IN, List<Object>> columnValuesFunction;
        private List<String> preSQLs = new ArrayList<>();
        private ExecutionConfig config;
        private HashMap<String, String> tableCacheConfig = TABLE_CACHE_DEFAULT;
        private HashMap<String, String> tableCompactionConfig = TABLE_COMPACTION_DEFAULT;
        private HashMap<String, String> tableCompressionConfig = TABLE_COMPRESSION_DEFAULT;
        private int tableGcGraceSecondsConfig = Table_Gc_Grace_Seconds_DEFAULT;
        private boolean createKeySpace = false;
        private boolean createTable = false;
        private String keySpaceClass = "SimpleStrategy";
        private int replicationFactor = 1;
        private String userName;
        private String password;
        private boolean useCredentials;
    }


    protected static class CassandraClusterBuilder extends ClusterBuilder implements Serializable {

        private String[] hosts;
        private String userName;
        private String password;
        private boolean useCredentials;

        public CassandraClusterBuilder(String hosts, boolean useCredentials, String userName, String password) {
            this.hosts = hosts.split(",");
            this.userName = userName;
            this.password = password;
            this.useCredentials = useCredentials;
        }

        @Override
        protected Cluster buildCluster(Cluster.Builder builder) {
            builder.addContactPoints(this.hosts);
            if (useCredentials && StringUtil.isNotEmpty(userName) && StringUtil.isNotEmpty(password)) {
                builder.withCredentials(userName, password);
            }
            return builder.build();
        }
    }

}
