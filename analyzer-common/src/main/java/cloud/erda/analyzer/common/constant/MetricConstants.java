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
 * 这个里面主要存放无法放入Field和Tag的常量
 */
public class MetricConstants {

    public static final String LEFT_SQUARE_BRACKET = "[";
    public static final String RIGHT_SQUARE_BRACKET = "]";
    public static final String LEFT_BRACKET = "(";
    public static final String RIGHT_BRACKET = ")";

    public static final String AGG = "agg";
    public static final String ENV = "env";
    public static final String STATUS_WINDOW_SIZE = "status.window.size";
    public static final String OUTAGE_WINDOW_SIZE = "outage.window.size";

    //alert event
    public static final String ALERT_TOPIC = "alert.topic";

    public static final String SPOT = "spot";
    public static final String TYPE_ID = "type_id";

    //type
    public static final String TYPE_STATUS = "status";
    public static final String TYPE_MACHINE = "machine";
    public static final String TYPE_ADDON = "addon";
    public static final String TYPE_DICE_COMPONENT = "dice_component";
    public static final String TYPE_APP_EXCEPTION = "app_exception";
    public static final String TYPE_APP_RESOURCE = "app_resource";
    public static final String TYPE_APP_TRANSACTION = "app_transaction";
    public static final String APPLICATION_HTTP = "application_http";
    public static final String APPLICATION_HTTP_SERVICE_ERROR = "application_http_service_error";
    public static final String APPLICATION_HTTP_PATH_ERROR = "application_http_path_error";
    public static final String APPLICATION_HTTP_SERVICE_RT = "application_http_service_rt";
    public static final String APPLICATION_HTTP_PATH_RT = "application_http_path_rt";

    //alert index
    public static final String MACHINE_LOAD5 = "machine_load5";
    public static final String MACHINE_DISK_UTIL = "machine_disk_util";
    public static final String MACHINE_CPU = "machine_cpu";
    public static final String MACHINE_DISK = "machine_disk";
    public static final String MACHINE_SWAP = "machine_swap";
    public static final String MACHINE_OUTAGE = "machine_outage";
    public static final String DICE_COMPONENT_CONTAINER_CPU = "dice_component_container_cpu";
    public static final String DICE_COMPONENT_CONTAINER_MEM = "dice_component_container_mem";
    public static final String DICE_COMPONENT_CONTAINER_OOM = "dice_component_container_oom";
    public static final String STATUS_CODE = "status_code";
    public static final String STATUS_RETRY = "status_retry";
    public static final String STATUS_LATENCY = "status_latency";
    public static final String STATUS_DIFF = "status_diff";
    public static final String APP_EXCEPTION_COUNT = "app_exception_count";
    public static final String APP_RESOURCE_CONTAINER_CPU = "app_resource_container_cpu";
    public static final String APP_RESOURCE_CONTAINER_MEM = "app_resource_container_mem";
    public static final String APP_RESOURCE_CONTAINER_OOM = "app_resource_container_oom";
    public static final String APP_RESOURCE_JVM_HEAP = "app_resource_jvm_heap";
    public static final String APP_RESOURCE_JVM_GC_TIME = "app_resource_jvm_gc_time";
    public static final String APP_RESOURCE_JVM_FULL_GC_COUNT = "app_resource_jvm_full_gc_count";
    public static final String APP_RESOURCE_NODEJS_HEAP = "app_resource_nodejs_heap";
    public static final String APP_TRANSACTION_HTTP_ERROR = "app_transaction_http_error";

    //target type
    public static final String TARGET_TYPE = "target_type";
    public static final String TARGET_ID = "target_id";
    public static final String TARGET_TYPE_CLUSTER = "cluster";
    public static final String TARGET_TYPE_PROJECT = "project";
    public static final String TARGET_TYPE_APPLICATION = "application";

    //metric name
    public static final String PRE_MACHINE_STATUS = "pre_machine_status";

    public static final String DISKIO = "diskio";
    public static final String PROCSTAT = "procstat";

    public static final String DISK = "disk";
    public static final String SYSTEM = "system";
    public static final String CPU = "cpu";
    public static final String MEM = "mem";
    public static final String CONTAINER_SUMMARY = "container_summary";
    public static final String DOCKER_CONTAINER_BLKIO = "docker_container_blkio";
    public static final String DOCKER_CONTAINER_SUMMARY = "docker_container_summary";
    public static final String HOST_SUMMARY = "host_summary";
    public static final String NET_PACKETS = "net_packets";

    public static final String PROCESSES = "processes";

    public static final String ERROR_ALERT = "error_alert";
    public static final String JVM_MEMORY = "jvm_memory";
    public static final String JVM_GC = "jvm_gc";
    public static final String NODEJS_MEMORY = "nodejs_memory";
    public static final String METASERVER_HOST = "host_metaserver";
    public static final String METASERVER_CONTAINER = "container_metaserver";


    //status-page metric name
    public static final String STATUS_PAGE = "status_page";

    public static final String N_CPUS = "n_cpus";

    //for alert calculate speed


    //source
    public static final String MACHINE = "machine";
    public static final String NETSTAT = "netstat";
    public static final String SWAP = "swap";
    public static final String MACHINE_SUMMARY = "machine_summary";
    public static final String LABELS = "labels";
    public static final String OFFLINE = "offline";

    public static final String MACHINE_STATUS = "machine_status";
    public static final String HOST_STATUS = "host_status";

    //钉钉告警
    public static final String PATH = "Path";
    public static final String TITLE = "Title";
    public static final String CONTENT_TIMESTAMP_UNIX = "Time";
    public static final String ALERT_COUNT = "alert_count";

    //网盘
    public static final String DISK_TYPE_NETDATA = "netdata";

    //告警level
    public static final String HIGH = "high";
    public static final String MEDIUM = "medium";

    //key name
    public static final String DISK_IO_UTIL = "disk_io_util";
    public static final String OUTAGE = "outage";
    public static final String LOAD5 = "load5";

    public static final String COUNT = "count";

    public static final String ALERT_TYPE = "alert_type";

    //addon
    public static final String ADDON_TOPIC = "addon.topic";

    //type
    public static final String META_TYPE = "Type";

    //平台组件相关的常量
    public static final String DOCKER = "docker";
    public static final String DOCKER_CONTAINER_CPU = "docker_container_cpu";
    public static final String DOCKER_CONTAINER_MEM = "docker_container_mem";
    public static final String DOCKER_CONTAINER_STATUS = "docker_container_status";
    public static final String DOCKER_CONTAINER_STATUS_SERVICE = "docker_container_status_service";
    public static final String DOCKER_CONTAINER_STATUS_COMPONENT = "docker_container_status_component";
    public static final String DOCKER_CONTAINER_MEM_SERVICE = "docker_container_mem_service";
    public static final String DOCKER_CONTAINER_MEM_COMPONENT = "docker_container_mem_component";
    public static final String DOCKER_CONTAINER_CPU_COMPONENT = "docker_container_cpu_component";
    public static final String DOCKER_CONTAINER_CPU_SERVICE = "docker_container_cpu_service";


    /**
     * Flink state keys
     */
    public static final String ALERT_EXPRESSION_STATE = "alert-expression";
    public static final String ALERT_KEY_FILTER_STATE = "alert-key-filter";
    public static final String METRIC_NAME_FILTER_STATE = "metric-name-filter";
    public static final String METRIC_KEYED_MAP_STATE = "metric-keyed-map";
    public static final String OFFLINE_MACHINE_FILTER_STATE = "offline-machine-filter";

    public static final String ALERT_NOTIFY_TARGETS_STATE = "alert-notify-targets";
    public static final String ALERT_NOTIFY_SETTINGS_STATE = "alert-notify-settings";
    public static final String ALERT_NOTIFY_STATUS_STATE = "alert-notify-status";
    public static final String ALERT_NOTIFY_STATE = "alert-notify-state";
    public static final String ALERT_NOTIFY_TEMPLATE_STATE = "alert-notify-state";

    public static final String TAG_TERMINUS_KEY = "terminus_key";
    public static final String TAG_CLUSTER_NAME = "cluster_name";

    public static final String ALERT_KEY = "alert_keys";
    public static final String ALERT_METRIC_NAME = "analyzer-alert";

    public static final String ALERT_ID = "alert_id";

    public static final String ALERT_TRIGER = "trigger";
    public static final String ALERT_TRIGER_ALERT = "alert";
    public static final String ALERT_TRIGER_RECOVER = "recover";

    public static final String ALERT_NOTIFY_DINGDING = "dingding";
    public static final String ALERT_NOTIFY_TICKET = "ticket";
    public static final String ALERT_NOTIFY_GROUP = "notify_group";

}
