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

package cloud.erda.analyzer.metrics.functions;

import cloud.erda.analyzer.common.constant.MetricConstants;
import cloud.erda.analyzer.common.constant.MetricTagConstants;
import cloud.erda.analyzer.common.models.MetricEvent;
import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.Map;

/**
 * @author liuhaoyang
 * @date 2020/11/2 15:15
 */
public class MachineMetricsReduceProcessFunction implements ReduceFunction<MetricEvent> {

    @Override
    public MetricEvent reduce(MetricEvent first, MetricEvent append) throws Exception {
        MetricEvent statusEvent = first;
        int count = 0;
        if (!MetricConstants.PRE_MACHINE_STATUS.equals(statusEvent.getName())) {
            statusEvent = new MetricEvent();
            statusEvent.setName(MetricConstants.PRE_MACHINE_STATUS);
            Map<String, String> tags = statusEvent.getTags();
            tags.put(MetricTagConstants.ORG_NAME, first.getTags().get(MetricTagConstants.ORG_NAME));
            tags.put(MetricTagConstants.CLUSTER_NAME, first.getTags().get(MetricTagConstants.CLUSTER_NAME));
            tags.put(MetricTagConstants.HOSTIP, first.getTags().get(MetricTagConstants.HOSTIP));
            tags.put(MetricTagConstants.HOST, first.getTags().get(MetricTagConstants.HOST));
            tags.put(MetricTagConstants.META, first.getTags().get(MetricTagConstants.META));
            tags.put(MetricTagConstants.METRIC_SCOPE, first.getTags().get(MetricTagConstants.METRIC_SCOPE));
            tags.put(MetricTagConstants.METRIC_SCOPE_ID, first.getTags().get(MetricTagConstants.METRIC_SCOPE_ID));
            if (first.getTags().containsKey(MetricTagConstants.LABELS)) {
                tags.put(MetricTagConstants.LABELS, first.getTags().get(MetricTagConstants.LABELS));
            }
            count++;
        }
        if (append.getTags().containsKey(MetricTagConstants.LABELS)) {
            statusEvent.getTags().put(MetricTagConstants.LABELS, append.getTags().get(MetricTagConstants.LABELS));
        }
        count++;
        Object reduceCount = statusEvent.getFields().get(MetricConstants.COUNT);
        reduceCount = reduceCount == null ? count : (Integer) reduceCount + count;
        statusEvent.getFields().put(MetricConstants.COUNT, reduceCount);
        statusEvent.setTimestamp(append.getTimestamp());
        return statusEvent;
    }
}
