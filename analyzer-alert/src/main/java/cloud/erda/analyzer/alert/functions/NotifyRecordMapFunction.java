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

package cloud.erda.analyzer.alert.functions;

import cloud.erda.analyzer.alert.models.NotifyRecord;
import cloud.erda.analyzer.alert.models.RenderedNotifyEvent;
import cloud.erda.analyzer.common.models.MetricEvent;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

public class NotifyRecordMapFunction implements MapFunction<RenderedNotifyEvent, NotifyRecord> {
    @Override
    public NotifyRecord map(RenderedNotifyEvent renderedNotifyEvent) throws Exception {
        val metric = renderedNotifyEvent.getMetricEvent();
        val record = new NotifyRecord();

        record.setGroupId(renderedNotifyEvent.getNotify().getTarget().getGroupId());
        record.setNotifyId(renderedNotifyEvent.getNotify().getNotifyId());
//        record.setNotifyId();
        record.setNotifyGroup(this.getTag(metric,"notify_group"));
        record.setScope(renderedNotifyEvent.getNotify().getScope());
        record.setScopeKey(renderedNotifyEvent.getNotify().getScopeId());
        record.setNotifyName(renderedNotifyEvent.getNotify().getNotifyName());
        record.setTitle(renderedNotifyEvent.getTitle());
        record.setNotifyTime(metric.getTimestamp() / 1000);
        return record;
    }
    private String getTag(MetricEvent metric,String key) {
        val value = metric.getTags().get(key);
        if (value == null) {
            return StringUtils.EMPTY;
        }
        return value;
    }
}
