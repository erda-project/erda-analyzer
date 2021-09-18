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
}
