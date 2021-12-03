/*
 * Copyright (c) 2021 Terminus, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cloud.erda.analyzer.common.constant;

/**
 * @author liuhaoyang
 * @date 2021/9/17 22:53
 */
public class SpanConstants {

    public static final String SPAN_METRIC_NAME = "span";

    public static final String TRACE_ID = "trace_id";

    public static final String SPAN_ID = "span_id";

    public static final String PARENT_SPAN_ID = "parent_span_id";

    public static final String OPERATION_NAME = "operation_name";

    public static final String START_TIME = "start_time";

    public static final String END_TIME = "end_time";

    public static final String TAG_HTTP_URL = "http_url";

    public static final String SPAN_LAYER = "span_layer";

    public static final String SPAN_LAYER_HTTP = "http";

    public static final String SPAN_LAYER_RPC = "rpc";

    public static final String SPAN_LAYER_CACHE = "cache";

    public static final String SPAN_LAYER_DB = "db";

    public static final String SPAN_LAYER_MQ = "mq";

    public static final String SPAN_LAYER_LOCAL = "local";

    public static final String SPAN_LAYER_UNKNOWN = "unknown";

    public static final String MESSAGE_BUS_DESTINATION = "message_bus_destination";

    public static final String DB_TYPE = "db_type";

    public static final String DB_TYPE_REDIS = "redis";

    public static final String PEER_SERVICE = "peer_service";

    public static final String SERVICE_NAME = "service_name";

    public static final String SERVICE_ID = "service_id";

    public static final String SERVICE_INSTANCE_IP = "service_instance_ip";

    public static final String ENV_ID = "env_id";

    public static final String SERVICE_INSTANCE_ID = "service_instance_id";

    public static final String SERVICE_INSTANCE_STARTED_AT = "service_instance_started_at";

    public static final String PROJECT_NAME = "project_name";

    public static final String WORKSPACE = "workspace";

    public static final String JAEGER_VERSION = "jaeger_version";

    public static final String JAEGER = "jaeger";

    public static final String SERVER = "server";

    public static final String SPAN_KIND = "span_kind";

    public static final String SPAN_KIND_SERVER = "server";

    public static final String SPAN_KIND_CLIENT = "client";

    public static final String SPAN_KIND_PRODUCER = "producer";

    public static final String SPAN_KIND_CONSUMER = "consumer";

    public static final String APPLICATION_SERVICE_NODE = "application_service_node";

    public static final String START_TIME_MEAN = "start_time_mean";

    public static final String START_TIME_COUNT = "start_time_count";

    public static final String INSTRUMENTATION_LIBRARY = "instrumentation_library";

    public static final String INSTRUMENTATION_LIBRARY_VERSION = "instrumentation_library_version";

    public static final String INSTRUMENT_SDK_VERSION = "instrument_sdk_version";

    public static final String INSTRUMENT_SDK = "instrument_sdk";

    public static final String TERMINUS_KEY = "terminus_key";

    public static final String ELAPSED = "elapsed";

    public static final String APPLICATION_HTTP = "application_http";

    public static final String APPLICATION_RPC = "application_rpc";

    public static final String APPLICATION_CACHE = "application_cache";

    public static final String APPLICATION_DB = "application_db";

    public static final String APPLICATION_MQ = "application_mq";

    public static final String TARGET_SERVICE_ID = "target_service_id";

    public static final String TARGET_SERVICE_NAME = "target_service_name";

    public static final String TARGET_TERMINUS_KEY = "target_terminus_key";

    public static final String TARGET_ENV_ID = "target_env_id";

    public static final String SOURCE_SERVICE_ID = "source_service_id";

    public static final String SOURCE_SERVICE_NAME = "source_service_name";

    public static final String SOURCE_TERMINUS_KEY = "source_terminus_key";

    public static final String SOURCE_ENV_ID = "source_env_id";

    public static final String HTTP_STATUS_CODE = "http_status_code";

    public static final String META = "_meta";

    public static final String METRIC_SCOPE = "_metric_scope";

    public static final String METRIC_SCOPE_ID = "_metric_scope_id";

    public static final String METRIC_SCOPE_MICRO_SERVICE = "micro_service";

    public static final String TRUE = String.valueOf(true);

    public static final String SOURCE_SERVICE_INSTANCE_ID = "source_service_instance_id";

    public static final String TARGET_SERVICE_INSTANCE_ID = "target_service_instance_id";

    public static final String REQUEST_ID = "request_id";

    public static final String TRACE_SAMPLED = "trace_sampled";

    public static final String ERROR = "error";

    public static final String TAG_HTTP_PATH = "http_path";
}
