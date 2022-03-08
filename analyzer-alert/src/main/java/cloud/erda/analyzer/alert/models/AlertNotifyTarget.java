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

package cloud.erda.analyzer.alert.models;

import com.fasterxml.jackson.annotation.JsonSetter;
import lombok.Data;

/**
 * @author: liuhaoyang
 * @create: 2020-01-01 23:53
 **/
@Data
public class AlertNotifyTarget {

    /**
     * //兼容历史数据（3.10 微服务）
     * public static final String ALERT_NOTIFY_TYPE_DINGDINGD = "dingding";
     * <p>
     * public static final String ALERT_NOTIFY_TYPE_NOTIFY_GROUP = "notify_group";
     * <p>
     * public static final String ALERT_NOTIFY_TYPE_TICKET = "ticket";
     * <p>
     * public static final String ALERT_NOTIFY_TYPE_HISTORY = "history";
     */
    private String type;

    @JsonSetter("group_id")
    private String groupId;

    @JsonSetter("group_type")
    private String groupType;

    @JsonSetter("dingding_url")
    private String dingdingUrl;

    @JsonSetter("level")
    private String level;

    private AlertLevel[] levels;

    private String[] groupTypes;
}
