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

package cloud.erda.analyzer.runtime.functions;

import cloud.erda.analyzer.common.constant.Constants;
import cloud.erda.analyzer.common.constant.MetricTagConstants;
import cloud.erda.analyzer.common.utils.JsonMapperUtils;
import cloud.erda.analyzer.runtime.models.KeyedMetricEvent;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * @author liuhaoyang
 * @date 2021/11/19 18:28
 */
public class MetricGroupTagProcessFunction implements MapFunction<KeyedMetricEvent, KeyedMetricEvent> {
    @Override
    public KeyedMetricEvent map(KeyedMetricEvent event) throws Exception {
        StringBuilder sb = new StringBuilder();
        HashMap<String, String> groups = new HashMap<>();
        for (String groupKey : event.getExpression().getGroup()) {
            Map<String, String> tags = event.getMetric().getTags();
            String value = tags.get(groupKey);
            sb.append(groupKey).append("_").append(value).append("_");
            groups.put(groupKey, value);
        }
        event.getMetric().addTag(MetricTagConstants.METRIC_EXPRESSION_GROUP, sb.toString());
        event.getMetric().addTag(MetricTagConstants.METRIC_EXPRESSION_GROUP_JSON, JsonMapperUtils.toStrings(groups));
        return event;
    }
}
