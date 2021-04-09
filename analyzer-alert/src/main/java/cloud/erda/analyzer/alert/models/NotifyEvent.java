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

import cloud.erda.analyzer.common.models.MetricEvent;
import lombok.Data;

@Data
public class NotifyEvent {
    private MetricEvent metricEvent;
    private Long notifyId; //sp_notify表的id
    private Notify notify; //用户创建的通知
    private UniversalTemplate notifyTemplate;
    private String templateId; //模版id
    //private String dingdingUrl;
//    private long timestamp;

    public NotifyEvent copy() {
        NotifyEvent notifyEvent = new NotifyEvent();
        notifyEvent.setMetricEvent(metricEvent);
        notifyEvent.setNotify(notify);
        notifyEvent.setNotifyId(notifyId);
        notifyEvent.setNotifyTemplate(notifyTemplate);
        notifyEvent.setTemplateId(templateId);
        return notifyEvent;
    }
}

//分组聚合
