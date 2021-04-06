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

import cloud.erda.analyzer.alert.models.NotifyEvent;
import cloud.erda.analyzer.common.models.MetricEvent;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

public class NotifyEventMapFunction implements FlatMapFunction<MetricEvent, NotifyEvent> {
    @Override
    public void flatMap(MetricEvent metricEvent, Collector<NotifyEvent> collector) {
        Map<String,String> tags = metricEvent.getTags();
        String notifyId = tags.get("id");
        if (StringUtils.isEmpty(notifyId)) {
            return;
        }
        String templateIds = tags.get("template_ids");
        templateIds = StringUtils.strip(templateIds,"[]");
        String[] templateIdArr = templateIds.split(",");
        for (String templateId : templateIdArr) {
            NotifyEvent notifyEvent = new NotifyEvent();
            notifyEvent.setNotifyId(Long.parseLong(notifyId));
            notifyEvent.setTemplateId(templateId);
            notifyEvent.setMetricEvent(metricEvent);
            collector.collect(notifyEvent);
        }
    }
}
