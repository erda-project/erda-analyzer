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

import cloud.erda.analyzer.common.constant.Constants;
import cloud.erda.analyzer.common.constant.MetricConstants;
import cloud.erda.analyzer.common.constant.MetricTagConstants;
import cloud.erda.analyzer.runtime.models.KeyedMetricEvent;
import org.apache.flink.api.java.functions.KeySelector;

import java.util.Map;

import static cloud.erda.analyzer.common.constant.MetricConstants.ALERT_ID;

/**
 * @author: liuhaoyang
 * @create: 2019-06-29 18:45
 **/
public class MetricGroupProcessFunction implements KeySelector<KeyedMetricEvent, String> {
    @Override
    public String getKey(KeyedMetricEvent value) throws Exception {
        /**
         * 处理表达式中的group
         * key 生成规则示例 2_alert_2_host_ip_10.168.0.83_window_1
         */
        String key = value.getKey();
        if (key == null) {
            key = key(value);
            value.setKey(key);
        }
        return key;
    }

    private String key(KeyedMetricEvent event) {
        return event.getMetadataId() + "_" + event.getMetric().getTags().get(MetricTagConstants.METRIC_EXPRESSION_GROUP) +
                "window_" + event.getExpression().getWindow();
    }
}
