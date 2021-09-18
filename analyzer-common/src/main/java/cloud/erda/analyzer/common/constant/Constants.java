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

package cloud.erda.analyzer.common.constant;

/**
 * @author liuhaoyang
 */
public class Constants {

    public static final String EMPTY_STRING = "";

    /**
     * kafka
     */
    public static final String TOPIC_ERROR = "kafka.topic.error";
    public static final String TOPIC_TRACE = "kafka.topic.trace";
    public static final String TOPIC_METRICS = "kafka.topic.metrics";
    public static final String METASERVER_TOPICS = "metaserver.topics";
    public static final String TOPIC_ANALYTICS = "kafka.topic.analytics";
    public static final String TOPIC_MARATHON = "kafka.topic.marathon";
    public static final String TOPIC_CONTAINER_LOG = "kafka.topic.log.container";
    public static final String TOPIC_JOB_LOG = "kafka.topic.log.job";
    public static final String TOPIC_ALERT = "kafka.topic.alert";
    public static final String TOPIC_NOTIFY = "kafka.topic.notify";
    public static final String TOPIC_OAP_TRACE = "kafka.topic.oap_trace";

    public static final String TOPIC_METRICS_TEMP = "kafka.topic.metrics_temp";

    //由monitor将记录存入mysql的topic
    public static final String TOPIC_RECORD_ALERT = "kafka.topic.alert.record";
    public static final String TOPIC_RECORD_NOTIFY = "kafka.topic.notify.record";

    public static final String KAFKA_BROKERS = "bootstrap.servers";

    /**
     * flink
     */
    public static final String FLINK_PARALLELISM_DEFAULT = "flink.parallelism.default";
    public static final String STREAM_PARALLELISM_INPUT = "stream.parallelism.input";
    public static final String STREAM_PARALLELISM_OUTPUT = "stream.parallelism.output";
    public static final String STREAM_PARALLELISM_OPERATOR = "stream.parallelism.operator";
    public static final String STREAM_WINDOW_SECONDS = "stream.window.seconds";
    public static final String STREAM_WINDOW_SIZE = "stream.window.size";
    public static final String STREAM_CHECKPOINT_INTERVAL = "stream.checkpoint.interval";
    public static final String STREAM_FS_STATE_BACKEND_PATH = "stream.state.backend.fs.path";
    public static final String STREAM_CHECKPOINT_ENABLE = "stream.checkpoint.enable";
    public static final String STREAM_STATE_BACKEND = "stream.state.backend";

    // MemoryStateBackend  . default
    public static final String STATE_BACKEND_MEMORY = "jobmanager";

    // FsStateBackend
    public static final String STATE_BACKEND_FS = "filesystem";

    // RocksDBStateBackend . prod default
    public static final String STATE_BACKEND_ROCKSDB = "rocksdb";

    /**
     * cassandra
     */
    public static final String ADDON_PARALLELISM = "addon.parallelism";
    public static final String STREAM_ASYNC = "stream.async";
    public static final String CASSANDRA_ADDR = "cassandra.addr";
    public static final String CASSANDRA_TTL = "cassandra.ttl";
    public static final String CASSANDRA_KEYSPACE = "cassandra.keyspace";
    public static final String CASSANDRA_BATCHSIZE = "cassandra.batchsize";
    public static final String CASSANDRA_KEYSPACE_REPLICATION = "cassandra.keyspace.replication";
    public static final String CASSANDRA_CHECKPOINT_KEYSPACE = "cassandra.checkpoint.keyspace";
    public static final String CASSANDRA_SECURITY_ENABLE = "cassandra.security.enable";
    public static final String CASSANDRA_SECURITY_USERNAME = "cassandra.security.username";
    public static final String CASSANDRA_SECURITY_PASSWORD = "cassandra.security.password";

    /**
     * notify silence policy
     */
    public static final long MaxSilence = 21600000;
    public static final String DoubledSilencePolicy = "doubled";
    public static final String CASSANDRA_TABLE_GC_GRACE_SECONDS = "cassandra.table.gc.grace.seconds";
    /**
     * es
     */
    public static final String ES_SECURITY_ENABLE = "es.security.enable";
    public static final String ES_SECURITY_USERNAME = "es.security.username";
    public static final String ES_SECURITY_PASSWORD = "es.security.password";
    public static final String ES_URL = "es.url";
    public static final String ES_INDEX_PREFIX = "es.index.prefix";
    public static final String ES_MAXRETRY_TIMEOUT = "es.maxretry.timeout";
    public static final String ES_REQUEST_CONNECT_TIMEOUT = "es.request.connect.timeout";
    public static final String ES_REQUEST_SOCKET_TIMEOUT = "es.request.socket.timeout";
    public static final String ES_REQUEST_CONN_REQUEST_TIMEOUT = "es.request.connectionrequest.timeout";

    /**
     * ES Index
     */
    public static final String MACHINE_SUMMARY_INDEX = "spot-machine_summary-full_cluster";
    /**
     * ES Query JSON
     */
    public static final String OFFLINE_MACHINE_QUERY = "{\"bool\":{\"filter\":[{\"term\":{\"fields"
            + ".labels\":\"offline\"}},{\"exists\":{\"field\":\"tags.terminus_version\"}}]}}}";

    /**
     * mysql
     */
    public static final String MYSQL_DATABASE = "mysql.database";
    public static final String MYSQL_HOST = "mysql.host";
    public static final String MYSQL_PASSWORD = "mysql.password";
    public static final String MYSQL_PORT = "mysql.port";
    public static final String MYSQL_USERNAME = "mysql.username";
    public static final String MYSQL_BATCH_SIZE = "mysql.batch.size";
    public static final String MYSQL_INTERVAL = "mysql.interval";

    /**
     * sql
     */
    public static final String ALERT_EXPRESSION_QUERY = "SELECT * FROM `sp_alert_expression` WHERE enable = 1";

    public static final String METRIC_EXPRESSION_QUERY = "SELECT * FROM `sp_metric_expression` WHERE enable = 1";

    public static final String ALERT_NOTIFY_QUERY = "SELECT * FROM `sp_alert_notify` WHERE enable = 1";
    //用户配置的告警策略
    public static final String NOTIFY_QUERY = "SELECT * FROM `sp_notify` WHERE enable = 1";

    public static final String ALERT_NOTIFY_TEMPLATE_QUERY = "SELECT * FROM `sp_alert_notify_template` WHERE enable = 1";

    public static final String ALERT_NOTIFY_CUSTOM_TEMPLATE_QUERY = "SELECT * FROM `sp_customize_alert_notify_template` WHERE enable = 1";

    /**
     * Script Invoker
     */
    public static final String INVOKE_FUNCTION_NAME = "invoke";
    public static final String ENGINE_NAME = "javascript";

    public static final String IS_RECOVER = "isRecover";

    public static final String SUMMARY_TAG_KEYS = "summary.tag.keys";

    /**
     * mysql source
     */
    public static final String METRIC_METADATA_INTERVAL = "metric.metadata.interval";

    public static final String METRIC_METADATA_TTL = "metric.metadata.ttl";

    public static final String NOTIFY_METADATA_INTERVAL = "notify.metadata.interval";

    public static final String NOTIFY_METADATA_TTL = "notify.metadata.ttl";

    /**
     * depends on
     */
    public static final String EVENTBOX_ADDR = "eventbox.addr";

    public static final String EVENTBOX_MESSAGE = "eventbox.message.path";

    public static final String CMDB_ADDR = "cmdb.addr";

    public static final String MONITOR_ADDR = "monitor.addr";

    /**
     * event box s
     */
    public static final String EVENTBOX_SENDER = "analyzer-alert";

    public static final String EVENTBOX_LABELS_DINGDINGD = "DINGDING";

    public static final String EVENTBOX_LABELS_MARKDOWN = "MARKDOWN";

    public static final String EVENTBOX_LABELS_GROUP = "GROUP";

    public static final String EVENTBOX_LABELS_TITLE = "title";

    /**
     * dice
     */
    public static final String DICE_SIZE = "dice.size";
    public static final String DICE_SIZE_TEST = "test";
    public static final String DICE_SIZE_PROD = "prod";

    public static final String META = "_meta";
    public static final String SCOPE = "_metric_scope";
    public static final String SCOPE_ID = "_metric_scope_id";

    public static final String MICRO_SERVICE = "micro_service";
}
