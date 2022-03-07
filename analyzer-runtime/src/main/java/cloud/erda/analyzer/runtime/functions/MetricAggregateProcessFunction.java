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
import cloud.erda.analyzer.common.utils.StringUtil;
import cloud.erda.analyzer.runtime.expression.functions.aggregators.Accumulator;
import cloud.erda.analyzer.runtime.expression.functions.aggregators.DefaultAccumulator;
import cloud.erda.analyzer.runtime.models.AggregatedMetricEvent;
import cloud.erda.analyzer.runtime.models.KeyedMetricEvent;
import cloud.erda.analyzer.runtime.scripts.ScriptInvoker;
import cloud.erda.analyzer.runtime.scripts.ScriptInvokerManager;
import cloud.erda.analyzer.runtime.utils.AggregateUtils;
import cloud.erda.analyzer.runtime.expression.functions.aggregators.AppliedFunctionAggregator;
import cloud.erda.analyzer.runtime.expression.functions.aggregators.FunctionAggregator;
import cloud.erda.analyzer.runtime.models.ExpressionFunction;
import cloud.erda.analyzer.runtime.models.ExpressionFunctionTrigger;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: liuhaoyang
 * @create: 2019-06-29 19:34
 **/
@Slf4j
public class MetricAggregateProcessFunction implements AggregateFunction<KeyedMetricEvent, Accumulator, AggregatedMetricEvent>, Serializable {

    @Override
    public Accumulator createAccumulator() {
        return new DefaultAccumulator();
    }

    @Override
    public Accumulator add(KeyedMetricEvent value, Accumulator accumulator) {

        if (!accumulator.initialized()) {
            accumulator.setMetadataId(value.getMetadataId());
            accumulator.setExpression(value.getExpression());

            for (Map.Entry<String, Object> attribute : value.getAttributes().entrySet()) {
                accumulator.setAttribute(attribute.getKey(), attribute.getValue().toString());
            }

            // 把 select 放到 attributes 中
            // select 优先级 高于 attribute
            for (Map.Entry<String, String> select : value.getExpression().getSelect().entrySet()) {
                accumulator.setAttribute(select.getKey(), select.getValue());
            }
        }

        accumulator.setMetric(value.getMetric());
        accumulator.setKey(value.getKey());

        /**
         * 执行表达式中 function 的 aggregator
         */
        for (ExpressionFunction function : value.getExpression().getFunctions()) {
            if (!ExpressionFunctionTrigger.applied.equals(function.getTrigger())) {
                continue;
            }
            FunctionAggregator aggregator = accumulator.getMetricAggregator(function);
            if (!(aggregator instanceof AppliedFunctionAggregator)) {
                aggregator = new AppliedFunctionAggregator(aggregator);
            }
            Object v = getFieldValue(value.getMetric(), value.getMetadataId(), function);
            aggregator.apply(v);
            accumulator.setFunction(aggregator, function);
        }

        return accumulator;
    }

    @Override
    public AggregatedMetricEvent getResult(Accumulator accumulator) {
        /**
         * 处理 trigger: aggregated
         */
        if (accumulator.getExpression().getFunctions().stream().anyMatch(function -> ExpressionFunctionTrigger.aggregated.equals(function.getTrigger()))) {
            Map<ExpressionFunction, FunctionAggregator> appliedFunctionAggregators = new HashMap<>();
            for (ExpressionFunction function : accumulator.getExpression().getFunctions()) {
                if (!ExpressionFunctionTrigger.applied.equals(function.getTrigger())) {
                    continue;
                }
                FunctionAggregator aggregator = accumulator.getMetricAggregator(function);
                appliedFunctionAggregators.put(function, aggregator);
            }
            for (ExpressionFunction function : accumulator.getExpression().getFunctions()) {
                if (!ExpressionFunctionTrigger.aggregated.equals(function.getTrigger())) {
                    continue;
                }
                FunctionAggregator aggregator = accumulator.getMetricAggregator(function);
                aggregator.apply(getFieldValue(accumulator.getMetric(), accumulator.getMetadataId(), function, appliedFunctionAggregators));
                accumulator.setFunction(aggregator, function);
            }
        }
        return accumulator.aggregate();
    }

    @Override
    public Accumulator merge(Accumulator a, Accumulator b) {
        return a.merge(b);
    }

    private Object getFieldValue(MetricEvent event, String metadataId, ExpressionFunction function) {
        if (StringUtil.isEmpty(function.getFieldScript())) {
            Map<String, Object> fields = event.getFields();
            return fields.get(function.getField());
        } else {
            try {
                ScriptInvoker invoker = ScriptInvokerManager.getFieldInvoker(metadataId + function.getField(), function.getFieldScript());
                return invoker.invoke(event.getFields(), event.getTags());
            } catch (Throwable throwable) {
                log.warn(String.format("Field script invoke fail. Script = [%s]", function.getFieldScript()), throwable);
                return null;
            }
        }
    }

    private Object getFieldValue(MetricEvent event, String metadataId, ExpressionFunction function, Map<ExpressionFunction, FunctionAggregator> functionAggregators) {
        Map<String, Object> fields = new HashMap<>(event.getFields());
        for (Map.Entry<ExpressionFunction, FunctionAggregator> entry : functionAggregators.entrySet()) {
            fields.put(AggregateUtils.getAggregatedField(entry.getKey()), entry.getValue().value());
        }
        if (StringUtil.isEmpty(function.getFieldScript())) {
            return fields.get(function.getField());
        } else {
            try {
                ScriptInvoker invoker = ScriptInvokerManager.getFieldInvoker(metadataId + function.getField(), function.getFieldScript());
                return invoker.invoke(fields, event.getTags());
            } catch (Throwable throwable) {
                log.warn(String.format("Field script invoke fail. Script = [%s]", function.getFieldScript()), throwable);
                return null;
            }
        }
    }
}
