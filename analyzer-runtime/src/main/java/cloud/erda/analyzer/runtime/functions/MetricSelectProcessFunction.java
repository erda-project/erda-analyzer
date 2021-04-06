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

package cloud.erda.analyzer.runtime.functions;

import cloud.erda.analyzer.common.constant.AlertConstants;
import cloud.erda.analyzer.common.models.MetricEvent;
import cloud.erda.analyzer.runtime.models.AggregateResult;
import cloud.erda.analyzer.runtime.models.AggregatedMetricEvent;
import cloud.erda.analyzer.runtime.models.OutputMetricEvent;
import cloud.erda.analyzer.runtime.utils.AggregateUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: liuhaoyang
 * @create: 2020-02-02 22:58
 **/
public interface MetricSelectProcessFunction {

    default MetricEvent mapAggregatedMetricEvent(AggregatedMetricEvent value) throws Exception {
        /**
         * 处理Expression中的 select 和 field result
         */
        OutputMetricEvent outputMetricEvent = new OutputMetricEvent();
        outputMetricEvent.setMetadataId(value.getMetadataId());
        outputMetricEvent.setAlias(value.getAlias());
        outputMetricEvent.setTimestamp(value.getMetric().getTimestamp());
        outputMetricEvent.setOutputs(value.getOutputs());
        outputMetricEvent.setRawMetricName(value.getMetric().getName());
        Map<String, Object> aggregatedFields = getAggregatedFields(value);
        outputMetricEvent.setAggregatedFields(aggregatedFields);
        outputMetricEvent.setAggregatedTags(getAggregatedTags(value, aggregatedFields));
        return mapOutputMetricEvent(outputMetricEvent);
    }

    MetricEvent mapOutputMetricEvent(OutputMetricEvent value) throws Exception;

    static Map<String, Object> getAggregatedFields(AggregatedMetricEvent value) {
        Map<String, Object> fields = new HashMap<>();
        for (AggregateResult result : value.getResults()) {
            String field = AggregateUtils.getAggregatedField(result.getFunction());
            if (!fields.containsKey(field)) {
                fields.put(field, result.getValue());
            }
            if (result.getFormattedValue() != null) {
                fields.put(field + "_format", result.getFormattedValue());
            }
        }
        return fields;
    }

    static Map<String, String> getAggregatedTags(AggregatedMetricEvent metricEvent, Map<String, Object> aggregatedFields) {
        Map<String, String> tags = new HashMap<>();
        Map<String, String> metricTags = metricEvent.getMetric().getTags();
        for (Map.Entry<String, String> attr : metricEvent.getAttributes().entrySet()) {
            String value = attr.getValue();
            if (value == null) {
                continue;
            }
            /**
             * 如果 select 的tag key 以 # 开头，那么映射原始 metric 的 tag 到 alert 中
             * 	"select": {
             * 		"cluster_name": "#cluster_name",
             *   }
             */

            if (value.startsWith("#")) {
                String originalTagKey = value.substring(1);
                if (metricTags.containsKey(originalTagKey)) {
                    tags.put(attr.getKey(), metricTags.get(originalTagKey));
                } else if (aggregatedFields.containsKey(originalTagKey)) {
                    Object fieldValue = aggregatedFields.get(originalTagKey);
                    if (fieldValue != null) {
                        tags.put(attr.getKey(), String.valueOf(fieldValue));
                    }
                }
            } else {
                tags.put(attr.getKey(), value);
            }
        }
        tags.put(AlertConstants.ALERT_EXPRESSION_ID, metricEvent.getMetadataId());
        return tags;
    }
}
