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

package cloud.erda.analyzer.runtime.expression.functions.aggregators;

import cloud.erda.analyzer.runtime.models.AggregateResult;
import cloud.erda.analyzer.runtime.models.AggregatedMetricEvent;
import cloud.erda.analyzer.runtime.models.Expression;
import cloud.erda.analyzer.runtime.models.ExpressionFunction;
import cloud.erda.analyzer.common.models.MetricEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static cloud.erda.analyzer.runtime.expression.functions.aggregators.FunctionAggregatorFactory.create;

/**
 * @author: liuhaoyang
 * @create: 2019-06-29 21:55
 **/
@Slf4j
public class DefaultAccumulator implements Accumulator {

    private final Map<String, FunctionAggregator> aggregators;
    private final Map<String, String> attributes;
    private final Map<FunctionAggregator, ExpressionFunction> functions;
    private String metadataId;
    private MetricEvent metricEvent;
    private String key;
    private Expression expression;

    public DefaultAccumulator() {
        aggregators = new HashMap<>();
        attributes = new HashMap<>();
        functions = new HashMap<>();
    }

    @Override
    public boolean initialized() {
        return metricEvent != null;
    }

    @Override
    public FunctionAggregator getMetricAggregator(ExpressionFunction function) {
        return aggregators.computeIfAbsent(function.getField() + "_" + function.getAggregator(), k -> create(function));
    }

    @Override
    public void setFunction(FunctionAggregator aggregator, ExpressionFunction function) {
        if (!functions.containsKey(aggregator)) {
            functions.put(aggregator, function);
        }
    }

    @Override
    public void setMetric(MetricEvent metricEvent) {
        this.metricEvent = metricEvent;
    }

    @Override
    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public void setAttribute(String key, String value) {
        attributes.put(key, value);
    }

    @Override
    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    @Override
    public String getMetadataId() {
        return metadataId;
    }

    @Override
    public Expression getExpression() {
        return this.expression;
    }

    @Override
    public MetricEvent getMetric() {
        return metricEvent;
    }

    @Override
    public void setMetadataId(String metadataId) {
        this.metadataId = metadataId;
    }

    @Override
    public AggregatedMetricEvent aggregate() {
        AggregatedMetricEvent event = new AggregatedMetricEvent();
        event.setMetadataId(metadataId);
        event.setMetric(metricEvent);
        event.setAttributes(new HashMap<>(attributes));
        event.setKey(key);
        event.setAlias(expression.getAlias());
        event.setOutputs(new HashSet<>(expression.getOutputs()));
        event.setResults(aggregators.values().stream().map(this::getAggregateResult).filter(Objects::nonNull).collect(Collectors.toList()));
        return event;
    }

    @Override
    public Accumulator merge(Accumulator other) {
        DefaultAccumulator otherAccumulator = (DefaultAccumulator) other;
        // merge attributes
        for (Map.Entry<String, String> attr : otherAccumulator.attributes.entrySet()) {
            setAttribute(attr.getKey(), attr.getValue());
        }

        // merge aggregators and functions
        for (Map.Entry<String, FunctionAggregator> item : otherAccumulator.aggregators.entrySet()) {
            FunctionAggregator aggregator = aggregators.get(item.getKey());
            if (aggregator == null) {
                aggregators.put(item.getKey(), item.getValue());
            } else {
                aggregator.merge(item.getValue());
            }
        }

        // merge functions
        for (FunctionAggregator aggregator : aggregators.values()) {
            ExpressionFunction function = otherAccumulator.functions.get(aggregator);
            if (function != null) {
                setFunction(aggregator, function);
            }
        }

        // merge MetricEvent
        if (this.metricEvent == null) {
            metricEvent = otherAccumulator.metricEvent;
        }

        //merge group
        if (this.key == null) {
            key = otherAccumulator.key;
        }

        return this;
    }

    private AggregateResult getAggregateResult(FunctionAggregator aggregator) {
        ExpressionFunction function = this.functions.get(aggregator);
        if (function == null) {
            return null;
        }
        AggregateResult result = new AggregateResult();
        result.setValue(aggregator.value());
        result.setFunction(function);
        return result;
    }
}
