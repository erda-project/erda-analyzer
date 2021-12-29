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

package cloud.erda.analyzer.tracing.functions;

import cloud.erda.analyzer.common.constant.SpanConstants;
import cloud.erda.analyzer.common.models.MetricEvent;
import cloud.erda.analyzer.common.utils.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @author liuhaoyang
 * @date 2021/9/23 10:20
 */
public class SlowOrErrorMetricFunction extends ProcessFunction<MetricEvent, MetricEvent> {

    private static final Map<String, String> parmaKeys;

    static {
        parmaKeys = new HashMap<>();
        parmaKeys.put(SpanConstants.APPLICATION_HTTP, "application.slow.http");
        parmaKeys.put(SpanConstants.APPLICATION_RPC, "application.slow.rpc");
        parmaKeys.put(SpanConstants.APPLICATION_CACHE, "application.slow.cache");
        parmaKeys.put(SpanConstants.APPLICATION_DB, "application.slow.db");
        parmaKeys.put("default", "application.slow.default");
    }

    private final ParameterTool parameterTool;

    public SlowOrErrorMetricFunction(ParameterTool parameterTool) {
        this.parameterTool = parameterTool;
    }

    @Override
    public void processElement(MetricEvent metricEvent, ProcessFunction<MetricEvent, MetricEvent>.Context context, Collector<MetricEvent> collector) throws Exception {
        long slowDefault = parameterTool.getLong(parmaKeys.get("default"));
        long metricSlow = slowDefault;

        if (parmaKeys.containsKey(metricEvent.getName())) {
            metricSlow = parameterTool.getLong(parmaKeys.get(metricEvent.getName()), slowDefault);
        }

        if ((long) metricEvent.getFields().get(SpanConstants.ELAPSED) > metricSlow) {
            MetricEvent slowMetric = metricEvent.copy();
            slowMetric.setName(metricEvent.getName() + "_slow");
            collector.collect(slowMetric);
        }

        String tagError = metricEvent.getTags().get(SpanConstants.ERROR);
        if (StringUtils.isNotEmpty(tagError) && tagError.equalsIgnoreCase(SpanConstants.TRUE)) {
            MetricEvent errorMetric = metricEvent.copy();
            errorMetric.setName(metricEvent.getName() + "_error");
            collector.collect(errorMetric);
        }

        metricEvent.getTags().remove(SpanConstants.TRACE_ID);
        metricEvent.getTags().remove(SpanConstants.REQUEST_ID);
        metricEvent.getTags().remove(SpanConstants.TRACE_SAMPLED);
        collector.collect(metricEvent);
    }
}
