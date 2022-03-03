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


public class MetricTagConstants {
    // meta tag
    public static final String META = "_meta";
    public static final String METRIC_SCOPE = "_metric_scope";
    public static final String METRIC_SCOPE_ID = "_metric_scope_id";

    //status-page metric tags
    public static final String STATUS_NAME = "status_name";
    public static final String METRIC = "metric";
    public static final String METRIC_ID = "metric_id";
    public static final String URL = "url";
    public static final String PROJECT_NAME = "project_name";
    public static final String PROJECT = "project";
    public static final String MESSAGE = "message";
    public static final String PROJECT_ID = "project_id";
    public static final String REQUEST_ID = "request_id";
    public static final String SERVICE_NAME = "service_name";
    public static final String METRIC_NAME = "metric_name";
    public static final String RAW_METRIC_NAME = "_raw_metric_name_";
    public static final String ALIAS = "_alias_";
    public static final String KEY = "key";
    public static final String LEVEL = "level";
    public static final String SOURCE = "source";


    public static final String HOSTIP = "host_ip";
    public static final String CLUSTER_NAME = "cluster_name";
    public static final String DEVICE = "device";
    public static final String HOST = "host";

    public static final String PROCESS_NAME = "process_name";
    public static final String PROCESS_PID = "pid";

    public static final String OFFLINE = "offline";

    //disk
    public static final String DISK_PATH = "path";
    public static final String DISK_TYPE = "type";

    //addon
    public static final String ID = "_id";
    public static final String ADDON_HOST = "host";
    public static final String ADDON_IP = "ip";

    //error
    public static final String RUNTIME_NAME = "runtime_name";
    public static final String APPLICATION_ID = "application_id";
    public static final String APPLICATION_NAME = "application_name";
    public static final String RUNTIME_ID = "runtime_id";
    public static final String ERROR_PATH = "path";
    public static final String TYPE = "type";
    public static final String CLASS = "class";
    public static final String WORKSPACE = "workspace";
    public static final String WORKSPACE_DEV = "DEV";
    public static final String WORKSPACE_PROD = "PROD";
    public static final String WORKSPACE_TEST = "TEST";
    public static final String LINE = "line";
    public static final String METHOD = "method";
    public static final String ISSUE_ID = "issue_id";
    public static final String ERROR_ID = "error_id";
    public static final String TERMINUS_KEY = "terminus_key";
    public static final String NAME = "name";

    //平台组件的tags
    public static final String COMPONENT_NAME = "component_name";
    public static final String INSTANCE_TYPE = "instance_type";
    public static final String CPU = "cpu";
    public static final String CONTAINER_ID = "container_id";
    public static final String INSTANCE_ID = "instance_id";
    public static final String PROD = "prod";
    public static final String ORG_ID = "ORG_ID";
    public static final String ORG_NAME = "ORG_NAME";

    //application http tag
    public static final String COMPONENT = "component";
    public static final String HTTP_METHOD = "http_method";
    public static final String HTTP_PATH = "http_path";
    public static final String HTTP_STATUS_CODE = "http_status_code";
    public static final String HTTP_URL = "http_url";
    public static final String SPAN_HOST = "span_host";
    public static final String PEER_HOSTNAME = "peer_hostname";
    public static final String TARGET_APPLICATION_ID = "target_application_id";
    public static final String TARGET_APPLICATION_NAME = "target_application_name";
    public static final String TARGET_INSTANCE_ID = "target_instance_id";
    public static final String TARGET_ORG_ID = "target_org_id";
    public static final String TARGET_PROJECT_ID = "target_project_id";
    public static final String TARGET_PROJECT_NAME = "target_project_name";
    public static final String TARGET_RUNTIME_ID = "target_runtime_id";
    public static final String TARGET_RUNTIME_NAME = "target_runtime_name";
    public static final String TARGET_SERVICE_NAME = "target_service_name";
    public static final String TARGET_TERMINUS_KEY = "target_terminus_key";
    public static final String TARGET_WORKSPACE = "target_workspace";

    public static final String WINDOW = "window";

    public static final String RECOVER = "recover";
    public static final String TRUE = "true";
    public static final String FALSE = "false";
    public static final String TRIGGER = "trigger";
    public static final String ALERT = "alert";
    public static final String TRIGGER_DURATION = "trigger_duration";
    public static final String TRIGGER_COUNT = "trigger_count";
    public static final String SILENCE_COUNT = "silence_count";
    public static final String SILENCE = "silence";

    public static final String LABELS = "labels";
    public static final String LABELS_OFFLINE = "offline";

    public static final String METRIC_EXPRESSION_GROUP = "metric_expression_group";
    public static final String METRIC_EXPRESSION_GROUP_JSON = "metric_expression_group_json";
}
