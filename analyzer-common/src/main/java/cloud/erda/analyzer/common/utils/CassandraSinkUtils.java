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

package cloud.erda.analyzer.common.utils;

import cloud.erda.analyzer.common.constant.Constants;
import cloud.erda.analyzer.common.sinks.TerminusCassandraSink;
import cloud.erda.analyzer.common.utils.annotations.ClusteringOrder;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.collect.Maps;
import cloud.erda.analyzer.common.utils.annotations.TableConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cassandra.shaded.com.google.common.reflect.TypeToken;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CassandraSinkUtils {

    /**
     * 封装大部分公共逻辑，简化CassandraSinks的使用
     */
    public static <T> void addSink(DataStream<T> dataStream, StreamExecutionEnvironment env, ParameterTool parameterTool) throws Exception {
        TerminusCassandraSink.addSink(dataStream)
                .enableWriteAheadLog()
                .setAsync(parameterTool.getBoolean(Constants.STREAM_ASYNC, false))
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OUTPUT, 5))
                .setHosts(parameterTool.getRequired(Constants.CASSANDRA_ADDR))
                .setKeyspace(parameterTool.getRequired(Constants.CASSANDRA_KEYSPACE))
                .setReplicationFactor(parameterTool.getInt(Constants.CASSANDRA_KEYSPACE_REPLICATION, 1))
                .setTtl(parameterTool.getInt(Constants.CASSANDRA_TTL, 0))
                .setBatchSize(parameterTool.getInt(Constants.CASSANDRA_BATCHSIZE, 200))
                .setCreateKeySpace(true)
                .setCreateTable(true)
                .setTableGcGraceSecondsConfig(getTableGcGraceSeconds(parameterTool))
                .useCredentials(parameterTool.getBoolean(Constants.CASSANDRA_SECURITY_ENABLE, false))
                .setUserName(parameterTool.get(Constants.CASSANDRA_SECURITY_USERNAME, ""))
                .setPassword(parameterTool.get(Constants.CASSANDRA_SECURITY_PASSWORD, ""))
                .build();
    }

    public static void excuteSql(ParameterTool parameterTool, String sql) {
        Session session = buildSession(parameterTool);
        session.execute(sql);
    }

    private static Session buildSession(ParameterTool parameterTool) {
        Cluster.Builder builder = new Cluster.Builder();
        builder.addContactPoint(parameterTool.get("cassandra.hosts"));
        Session session = builder.build().connect();
        return session;
    }

    public static String buildCreateTableSql(TerminusCassandraSink.CassandraSinkParam param, MappingManager mappingManager, Class cls) throws NoSuchFieldException, IllegalAccessException, InstantiationException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException {
        Map<String, String> clusterKeyOrderMap = buildClusterKeyOrderMap(cls);

        Map<String, String> clusterKeyNameMap = Maps.newLinkedHashMap();

        Mapper mapper = mappingManager.mapper(cls);
        Field mapperField = mapper.getClass().getDeclaredField("mapper");
        mapperField.setAccessible(true);
        Object actualMapper = mapperField.get(mapper);

        //allColumns
        java.lang.reflect.Method allColumnsM = actualMapper.getClass().getMethod("allColumns");
        allColumnsM.setAccessible(true);
        List allColumns = (List) allColumnsM.invoke(actualMapper);

        //keyspace
        java.lang.reflect.Method getKeyspaceM = actualMapper.getClass().getMethod("getKeyspace");
        getKeyspaceM.setAccessible(true);
        String keyspace = getKeyspaceM.invoke(actualMapper).toString();

        //table
        java.lang.reflect.Method getTableM = actualMapper.getClass().getMethod("getTable");
        getTableM.setAccessible(true);
        String table = getTableM.invoke(actualMapper).toString();


        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("create table IF NOT EXISTS  ")
                .append(keyspace)
                .append(".")
                .append(table).append("(");

        List partitionKeys = new ArrayList<String>();   //partition keys
        List clusteringKeys = new ArrayList<String>();  //clustering keys

        for (int i = 0; i < allColumns.size(); i++) {

            //fieldName
            java.lang.reflect.Field columnNameF = allColumns.get(i).getClass().getSuperclass().getDeclaredField("columnName");
            columnNameF.setAccessible(true);
            String columnName = columnNameF.get(allColumns.get(i)).toString();
            java.lang.reflect.Field fieldNameF = allColumns.get(i).getClass().getSuperclass().getDeclaredField("fieldName");
            fieldNameF.setAccessible(true);
            String fieldName = fieldNameF.get(allColumns.get(i)).toString();

            //fieldType
            java.lang.reflect.Field filedTypeF = allColumns.get(i).getClass().getSuperclass().getDeclaredField("fieldType");
            filedTypeF.setAccessible(true);
            TypeToken typeToken = (TypeToken) filedTypeF.get(allColumns.get(i));

            //kind
            java.lang.reflect.Field kindF = allColumns.get(i).getClass().getSuperclass().getDeclaredField("kind");
            kindF.setAccessible(true);
            Object columnKind = kindF.get(allColumns.get(i));

            stringBuffer.append(columnName).append("\t");
            Class t = typeToken.getRawType();
            stringBuffer.append(transformToColumnType(t.getCanonicalName(),
                    columnKind.toString().equals("PARTITION_KEY") || columnKind.toString().equals("CLUSTERING_COLUMN"))).append(",");

            if (columnKind.toString().equals("PARTITION_KEY")) {
                partitionKeys.add(columnName);
            }
            if (columnKind.toString().equals("CLUSTERING_COLUMN")) {
                clusteringKeys.add(columnName);
                clusterKeyNameMap.put(fieldName, columnName);
            }
        }

        if (allColumns.size() <= 0 || partitionKeys.size() <= 0) {
            throw new IllegalArgumentException("You need to specify at least one column and one partition key !");
        }

        stringBuffer.append("PRIMARY KEY ((");
        stringBuffer.append(String.join(",", partitionKeys));
        stringBuffer.append(")");

        if (clusteringKeys.size() > 0) {
            stringBuffer.append(",");
            stringBuffer.append(String.join(",", clusteringKeys));
        }
        stringBuffer.append("))");
        if (!clusterKeyNameMap.isEmpty()) {
            stringBuffer.append("WITH CLUSTERING ORDER BY (");
            StringBuffer orderbuffer = new StringBuffer();
            for (Map.Entry<String, String> fieldNameColumnName : clusterKeyNameMap.entrySet()) {
                String fieldName = fieldNameColumnName.getKey();
                orderbuffer.append(fieldNameColumnName.getValue())
                        .append(" ")
                        .append(clusterKeyOrderMap.getOrDefault(fieldName, "ASC"))
                        .append(",");
            }
            stringBuffer.append(orderbuffer.substring(0, orderbuffer.length() - 1)).append(")").toString();
        } else {
            stringBuffer.append("WITH ");
        }
        return stringBuffer.append(buildFromParam(param, !clusterKeyNameMap.isEmpty())).append(buildConfig(cls)).toString();
    }

    private static String buildFromParam(TerminusCassandraSink.CassandraSinkParam param, Boolean needAnd) {
        StringBuffer stringBuffer = new StringBuffer();
        HashMap<String, String> compression = param.getTableCompressionConfig();
        if (needAnd) {
            stringBuffer.append("and compression={");
        } else {
            stringBuffer.append(" compression={");

        }
        for (Map.Entry<String, String> kv : compression.entrySet()) {
            if (kv.getValue() != null) {
                stringBuffer.append("'").append(kv.getKey()).append("'")
                        .append(":").append("'").append(kv.getValue()).append("'").append(",");
            }
        }
        stringBuffer.replace(stringBuffer.length() - 1, stringBuffer.length(), "")
                .append("}").append("\n");

        HashMap<String, String> cache = param.getTableCacheConfig();
        stringBuffer.append("and caching={");
        for (Map.Entry<String, String> kv : cache.entrySet()) {
            if (kv.getValue() != null) {
                stringBuffer.append("'").append(kv.getKey()).append("'")
                        .append(":").append("'").append(kv.getValue()).append("'").append(",");
            }
        }
        stringBuffer.replace(stringBuffer.length() - 1, stringBuffer.length(), "")
                .append("}").append("\n");

        HashMap<String, String> compaction = param.getTableCompactionConfig();
        stringBuffer.append("and compaction={");
        for (Map.Entry<String, String> kv : compaction.entrySet()) {
            if (kv.getValue() != null) {
                stringBuffer.append("'").append(kv.getKey()).append("'")
                        .append(":").append("'").append(kv.getValue()).append("'").append(",");
            }
        }
        stringBuffer.replace(stringBuffer.length() - 1, stringBuffer.length(), "")
                .append("}").append("\n");

        stringBuffer.append("and gc_grace_seconds = ").append(param.getTableGcGraceSecondsConfig()).append("\n");

        return stringBuffer.toString();
    }

    private static String buildConfig(Class cls) {
        StringBuffer stringBuffer = new StringBuffer();
        TableConfig tableConfig = (TableConfig) cls.getAnnotation(TableConfig.class);
        if (tableConfig == null) {
            return "";
        }
        stringBuffer.append("and default_time_to_live = ").append(tableConfig.defaultTimeToLive()).append("\n");

        stringBuffer.append("and bloom_filter_fp_chance = ").append(tableConfig.bloomFilterFpChance()).append("\n");

        stringBuffer.append("and comment = '").append(tableConfig.comment()).append("'\n");

        stringBuffer.append("and crc_check_chance = ").append(tableConfig.crcCheckChance()).append("\n");

        stringBuffer.append("and dclocal_read_repair_chance = ").append(tableConfig.dclocalReadRepairChance()).append("\n");

//        stringBuffer.append("and gc_grace_seconds = ").append(tableConfig.gcGraceSeconds()).append("\n");

        stringBuffer.append("and max_index_interval = ").append(tableConfig.maxIndexInterval()).append("\n");

        stringBuffer.append("and memtable_flush_period_in_ms = ").append(tableConfig.memtableFlushPeriodInMs()).append("\n");

        stringBuffer.append("and min_index_interval = ").append(tableConfig.minIndexInterval()).append("\n");

        stringBuffer.append("and read_repair_chance = ").append(tableConfig.readRepairChance()).append("\n");

        stringBuffer.append("and speculative_retry = '").append(tableConfig.speculativeRetry()).append("'\n");

        return stringBuffer.toString();

    }

    private static Map<String, String> buildClusterKeyOrderMap(Class cls) {

        Field[] fields = cls.getDeclaredFields();

        Map<String, String> clusterKeyOrderMap = Maps.newHashMap();
        for (Field field : fields) {
            field.setAccessible(true);
            if (field.isAnnotationPresent(ClusteringOrder.class)) {
                ClusteringOrder clusteringOrder = field.getAnnotation(ClusteringOrder.class);

                clusterKeyOrderMap.put(field.getName(), clusteringOrder.value().name());
            }
        }
        return clusterKeyOrderMap;
    }

    //根据参数构建keyspace创建
    public static String buildCreateKeySpaceSql(TerminusCassandraSink.CassandraSinkParam param) {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("create keyspace if not EXISTS  ").append(param.getKeySpace())
                .append(" with replication={'class':'")
                .append(param.getKeySpaceClass())
                .append("', 'replication_factor':")
                .append(param.getReplicationFactor()).append("};");
        return stringBuffer.toString();
    }

    public static String transformToColumnType(String obj, Boolean key) {
        String columnType = "text";

        switch (obj) {
            case "java.lang.String":
                columnType = "text";
                break;
            case "long":
            case "java.lang.Long":
                columnType = "bigint";
                break;
            case "java.nio.ByteBuffer":
                columnType = "blob";
                break;
            case "boolean":
            case "java.lang.Boolean":
                columnType = "boolean";
                break;
            case "double":
            case "java.lang.Double":
                columnType = "double";
                break;
            case "short":
            case "java.lang.Short":
                columnType = "smallint";
                break;
            case "int":
            case "java.lang.Integer":
                columnType = "int";
                break;
            case "java.util.UUID":
                columnType = "timeuuid";
                break;
            case "byte":
            case "java.lang.Byte":
                columnType = "tinyint";
                break;
            case "java.math.BigInteger":
                columnType = "varint";
                break;
            case "float":
            case "java.lang.Float":
                columnType = "float";
                break;
            case "java.net.InetAddress":
                columnType = "inet";
                break;
            case "java.math.BigDecimal":
                columnType = "decimal";
                break;
            case "java.util.Date":
                columnType = "timestamp";
                break;
            case "java.time.LocalDate":
                columnType = "date";
                break;
            case "java.util.List":
                columnType = key ? "frozen<list<text>>" : "list<text>";
                break;
            case "java.util.Set":
                columnType = key ? "frozen<set<text>>" : "set<text>";
                break;
            case "java.util.Map":
                columnType = key ? "frozen<map<text,text>>" : "map<text,text>";
                break;
            case "com.datastax.driver.core.TupleValue":
                columnType = "tuple";
                break;
            default:
                break;
        }
        return columnType;
    }

    private static int getTableGcGraceSeconds(ParameterTool parameterTool) {
        String diceSize = parameterTool.get(Constants.DICE_SIZE, Constants.DICE_SIZE_PROD);
        if (diceSize.equals(Constants.DICE_SIZE_TEST)) {
            return 0;
        }
        return parameterTool.getInt(Constants.CASSANDRA_TABLE_GC_GRACE_SECONDS, 86400);
    }
}
