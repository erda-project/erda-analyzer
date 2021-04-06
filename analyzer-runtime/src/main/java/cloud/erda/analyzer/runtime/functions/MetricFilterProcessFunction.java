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

import cloud.erda.analyzer.common.models.MetricEvent;
import cloud.erda.analyzer.runtime.expression.filters.FilterOperator;
import cloud.erda.analyzer.runtime.expression.filters.FilterOperatorFactory;
import cloud.erda.analyzer.runtime.models.ExpressionFilter;
import cloud.erda.analyzer.runtime.models.ExpressionMetadata;
import cloud.erda.analyzer.runtime.models.KeyedMetricEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: liuhaoyang
 * @create: 2019-06-29 14:16
 **/
@Slf4j
public class MetricFilterProcessFunction extends ExpressionBroadcastProcessFunction<MetricEvent, KeyedMetricEvent> {

    private MapStateDescriptor<String, Map<String, Long>> nameMapStateDescriptor;
    private MapStateDescriptor<String, ExpressionMetadata> expressionStateDescriptor;
    private long stateTtl;
    private long lastCleanTime;
    private transient Meter meter;

    public MetricFilterProcessFunction(
            long stateTtl,
            MapStateDescriptor<String, Map<String, Long>> nameMapState,
            MapStateDescriptor<String, ExpressionMetadata> expressionState) {
        this.stateTtl = stateTtl;
        this.nameMapStateDescriptor = nameMapState;
        this.expressionStateDescriptor = expressionState;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        lastCleanTime = System.currentTimeMillis();
//        this.meter = getRuntimeContext().getMetricGroup()
//                .meter("filter_qps", new MeterView(20));
    }

    @Override
    public void processElement(MetricEvent value, ReadOnlyContext ctx, Collector<KeyedMetricEvent> out) throws Exception {
        /**
         * 处理表达式中的filter
         * 	"metric":"cpu"
         * 	"window": 5,
         * 	"filters":[
         *        {
         * 	        "tag" : "project_id",
         * 	        "operator" : "eq",
         * 	        "value" : "1"
         *        },
         *        {
         * 	        "tag" : "cluster_name",
         * 	        "operator" : "eq",
         * 	        "value" : "y"
         *        }
         * 	]
         *   ...
         */
//        this.meter.markEvent();
        ReadOnlyBroadcastState<String, ExpressionMetadata> exprState = ctx.getBroadcastState(expressionStateDescriptor);
        ReadOnlyBroadcastState<String, Map<String, Long>> nameMapState = ctx.getBroadcastState(nameMapStateDescriptor);
        Map<String, Long> exprIdSet = nameMapState.get(value.getName());
        if (exprIdSet != null) {
            for (Map.Entry<String, Long> exprId : exprIdSet.entrySet()) {
                ExpressionMetadata metadata = exprState.get(exprId.getKey());
                if (metadata == null || !metadata.isEnable()) {
                    continue;
                }
                if (!filter(value, metadata)) {
                    continue;
                }
                KeyedMetricEvent event = new KeyedMetricEvent();
                event.setAttributes(metadata.getAttributes());
                event.setMetric(value);
                event.setMetadataId(metadata.getId());
                event.setExpression(metadata.getExpression());
                out.collect(event);
            }
        }
    }

    @Override
    public void processBroadcastElement(ExpressionMetadata value, Context ctx, Collector<KeyedMetricEvent> out) throws Exception {
        /**
         * delete 过期的metadata
         */
        cleanMetadata(ctx);

        if (value == null) {
            return;
        }
        BroadcastState<String, Map<String, Long>> nameMapState = ctx.getBroadcastState(nameMapStateDescriptor);
        for (String metric : value.getExpression().getMetrics()) {
            Map<String, Long> exprIdSet = nameMapState.get(metric);
            if (exprIdSet == null) {
                exprIdSet = new HashMap<>();
                nameMapState.put(metric, exprIdSet);
            }
            if (value.isEnable()) {
                exprIdSet.put(value.getId(), value.getProcessingTime());
            } else {
                exprIdSet.remove(value.getId());
            }
        }

        ctx.getBroadcastState(expressionStateDescriptor).put(value.getId(), value);
    }

    private void cleanMetadata(Context ctx) throws Exception {
        long now = System.currentTimeMillis();
        if (now - lastCleanTime < stateTtl) {
            return;
        }

        BroadcastState<String, ExpressionMetadata> exprState = ctx.getBroadcastState(expressionStateDescriptor);
        Map<String, ExpressionMetadata> expressionMetadataImmutableEntries = new HashMap<>();
        exprState.immutableEntries().forEach(x -> expressionMetadataImmutableEntries.put(x.getKey(), x.getValue()));

        for (Map.Entry<String, ExpressionMetadata> item : expressionMetadataImmutableEntries.entrySet()) {
            if (now - item.getValue().getProcessingTime() > stateTtl) {
                exprState.remove(item.getKey());
            }
        }

        BroadcastState<String, Map<String, Long>> nameMapState = ctx.getBroadcastState(nameMapStateDescriptor);

        for (Map.Entry<String, Map<String, Long>> item : nameMapState.immutableEntries()) {
            for (Map.Entry<String, Long> subItem : new HashMap<>(item.getValue()).entrySet()) {
                if (now - subItem.getValue() > stateTtl) {
                    item.getValue().remove(subItem.getKey());
                }
            }
        }

        lastCleanTime = now;
        log.info("Clean up expired metric metadata.");
    }

    private boolean filter(MetricEvent event, ExpressionMetadata metadata) {
        for (ExpressionFilter filter : metadata.getExpression().getFilters()) {
            FilterOperator operator = FilterOperatorFactory.create(filter, metadata);
            Map<String, String> tags = event.getTags();
            if (!operator.invoke(filter, tags.get(filter.getTag()))) {
                return false;
            }
        }
        return true;
    }
}
