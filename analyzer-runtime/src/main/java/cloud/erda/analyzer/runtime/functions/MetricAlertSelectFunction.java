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
import cloud.erda.analyzer.common.utils.JsonMapperUtils;
import cloud.erda.analyzer.runtime.models.AggregateResult;
import cloud.erda.analyzer.runtime.models.AggregatedMetricEvent;
import cloud.erda.analyzer.runtime.models.ExpressionFunction;
import cloud.erda.analyzer.runtime.models.OutputMetricEvent;
import cloud.erda.analyzer.common.constant.MetricTagConstants;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static cloud.erda.analyzer.common.constant.MetricConstants.ALERT_METRIC_NAME;

@Slf4j
public class MetricAlertSelectFunction extends KeyedProcessFunction<String, AggregatedMetricEvent, MetricEvent> implements MetricSelectProcessFunction {

    private ValueStateDescriptor<RecoverState> recoverStateDescriptor;

    public MetricAlertSelectFunction() {
        recoverStateDescriptor = new ValueStateDescriptor<RecoverState>("metric-alert-select-state", TypeInformation.of(RecoverState.class));
    }

    public boolean filter(AggregatedMetricEvent value) throws Exception {
        boolean hasRecoverAttribute = Boolean.parseBoolean(value.getAttributes().getOrDefault(MetricTagConstants.RECOVER, MetricTagConstants.FALSE));
        if (!hasRecoverAttribute) {
            return value.isOperatorResult();
        }
        ValueState<RecoverState> state = getRuntimeContext().getState(recoverStateDescriptor);
        RecoverState recoverState = state.value();
        if (recoverState == null) {
            recoverState = factory(value.getKey());
        }

        long lastTimestamp = recoverState.getTimestamp();
        long now = System.currentTimeMillis();

        if (value.isOperatorResult()) {
            value.getAttributes().put(MetricTagConstants.TRIGGER, MetricTagConstants.ALERT);
            value.getAttributes().put(MetricTagConstants.TRIGGER_DURATION, String.valueOf(now - lastTimestamp));
            if (!recoverState.isNeedRecover()) {
                recoverState.setNeedRecover(true);
                recoverState.setTimestamp(now);
                state.update(recoverState);
            }
            return true;
        }
        if (!value.isOperatorResult() && recoverState.isNeedRecover()) {
            recoverState.setNeedRecover(false);
            recoverState.setTimestamp(now);
            value.getAttributes().put(MetricTagConstants.TRIGGER, MetricTagConstants.RECOVER);
            value.getAttributes().put(MetricTagConstants.TRIGGER_DURATION, String.valueOf(now - lastTimestamp));
            state.update(recoverState);
            return true;
        }
        return false;
    }

    @Override
    public void processElement(AggregatedMetricEvent value, Context context, Collector<MetricEvent> out) throws Exception {
        if (filter(value)) {
            MetricEvent alert = mapAggregatedMetricEvent(value);
            try {
                if (log.isInfoEnabled()) {
                    log.info("Collect alert event --> {}", JsonMapperUtils.toStrings(value));
                }
                out.collect(alert);
            } catch (Throwable throwable) {
                log.error("Cannot collect alertEvent from {} , tags {}", value.getMetric().getName(), JsonMapperUtils.toStrings(alert.getTags()), throwable);
                throw throwable;
            }
        }
    }

    @Override
    public MetricEvent mapOutputMetricEvent(OutputMetricEvent value) throws Exception {
        MetricEvent alertEvent = new MetricEvent();
        alertEvent.setName(ALERT_METRIC_NAME);
        alertEvent.setTimestamp(value.getTimestamp());
        alertEvent.getTags().putAll(value.getAggregatedTags());
        alertEvent.getFields().putAll(value.getAggregatedFields());
        alertEvent.getTags().put(MetricTagConstants.RAW_METRIC_NAME, value.getRawMetricName());
        alertEvent.getTags().put(MetricTagConstants.ALIAS, value.getAlias());
        if (!alertEvent.getTags().containsKey(MetricTagConstants.TRIGGER)) {
            alertEvent.getTags().put(MetricTagConstants.TRIGGER, MetricTagConstants.ALERT);
        }
        if (alertEvent.getTags().containsKey(MetricTagConstants.TRIGGER_DURATION)) {
            alertEvent.getFields().put(MetricTagConstants.TRIGGER_DURATION, Long.parseLong(alertEvent.getTags().get(MetricTagConstants.TRIGGER_DURATION)));
            alertEvent.getTags().remove(MetricTagConstants.TRIGGER_DURATION);
        }

        if (value.getMetric().getMetric().getTags().containsKey(MetricTagConstants.METRIC_EXPRESSION_GROUP_JSON)) {
            alertEvent.getTags().put(AlertConstants.ALERT_SUBJECT, value.getMetric().getMetric().getTags().get(MetricTagConstants.METRIC_EXPRESSION_GROUP_JSON));
        }

        if (value.getMetric().isOperatorResult()) {
            List<ExpressionFunction> funcs = new ArrayList<>();
            for (AggregateResult result : value.getMetric().getResults()) {
                if (result.isOperatorResult()) {
                    funcs.add(result.getFunction());
                }
            }
            alertEvent.getFields().put(AlertConstants.ALERT_TRIGGER_FUNCTIONS, JsonMapperUtils.toStrings(funcs));
        }

        alertEvent.getTags().put(AlertConstants.ALERT_EVENT_FAMILY_ID, DigestUtils.md5Hex(String.format("%s_%s_%s_%s",
                value.getAggregatedTags().get(AlertConstants.ALERT_ID),
                value.getAggregatedTags().get(AlertConstants.ALERT_TYPE),
                value.getAggregatedTags().get(AlertConstants.ALERT_INDEX),
                value.getMetric().getMetric().getTags().get(MetricTagConstants.METRIC_EXPRESSION_GROUP))));

        return alertEvent;
    }

    private RecoverState factory(String key) {
        RecoverState recoverState = new RecoverState();
        recoverState.setId(key);
        recoverState.setNeedRecover(false);
        recoverState.setTimestamp(System.currentTimeMillis());
        return recoverState;
    }

    @Data
    public static class RecoverState implements Serializable {

        private String id;

        private boolean needRecover;

        private long timestamp;
    }
}
