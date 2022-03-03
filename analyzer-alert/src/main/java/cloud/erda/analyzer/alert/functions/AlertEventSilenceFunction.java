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

import cloud.erda.analyzer.alert.models.AlertEvent;
import cloud.erda.analyzer.alert.models.AlertEventNotifyMetric;
import cloud.erda.analyzer.alert.models.AlertLevel;
import cloud.erda.analyzer.alert.models.AlertTrigger;
import cloud.erda.analyzer.alert.utils.OutputTagUtils;
import cloud.erda.analyzer.common.constant.Constants;
import cloud.erda.analyzer.common.constant.MetricTagConstants;
import lombok.Data;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author: liuhaoyang
 * @create: 2020-01-05 23:13
 **/

public class AlertEventSilenceFunction extends KeyedProcessFunction<String, AlertEvent, AlertEvent> {

    private ValueStateDescriptor<SilenceState> silenceStateDescriptor;

    public AlertEventSilenceFunction() {
        silenceStateDescriptor = new ValueStateDescriptor<SilenceState>("alert-event-silence-state", TypeInformation.of(SilenceState.class));
    }

    @Override
    public void processElement(AlertEvent value, Context ctx, Collector<AlertEvent> out) throws Exception {
        AlertEventNotifyMetric processMetric = AlertEventNotifyMetric.createFrom(value.getMetricEvent());
        ValueState<SilenceState> state = getRuntimeContext().getState(silenceStateDescriptor);
        SilenceState silence = state.value();
        if (silence == null) {
            silence = new SilenceState();
            silence.setLastTimestamp(0);
            silence.setTrigger(AlertTrigger.alert);
            silence.setTriggerCount(0);
            silence.setSilenceCount(0);
            silence.setSilence(value.getAlertNotify().getSilence());
            silence.setLastAlertLevel(value.getLevel());
            state.update(silence);
        }

        // process eventCount and silence
        if (AlertTrigger.alert.equals(value.getTrigger())) {
            if (ctx.timestamp() - silence.getLastTimestamp() < silence.getSilence()) {
                // If the alert level is higher than the alert level triggered last time, the alert event will continue to be process
                if (silence.getLastAlertLevel().compareTo(value.getLevel()) <= 0) {
                    silence.setSilenceCount(silence.getSilenceCount() + 1);
                    processMetric.addSilenced();
                    ctx.output(OutputTagUtils.AlertEventNotifyProcess, processMetric);
                    return;
                }
            }
            silence.setTriggerCount(silence.getTriggerCount() + 1);
        } else {
            silence.setTriggerCount(0);
            silence.setSilence(value.getAlertNotify().getSilence());
            silence.setSilenceCount(0);
            // double check recover event
            if (AlertTrigger.recover.equals(silence.getTrigger())) {
                return;
            }
        }

        // set silence with doubled policy
        if (value.getAlertNotify().getSilencePolicy().equals(Constants.DoubledSilencePolicy)) {
            if (value.getAlertNotify().getSilence() * Math.pow(2, silence.getTriggerCount()) < Constants.MaxSilence) {
                silence.setSilence((long) (value.getAlertNotify().getSilence() * Math.pow(2, silence.getTriggerCount())));
            } else {
                silence.setSilence(Constants.MaxSilence);
            }
        } else {
            silence.setSilence(value.getAlertNotify().getSilence());
        }

        value.getMetricEvent().getFields().put(MetricTagConstants.SILENCE_COUNT, silence.getSilenceCount());
        value.getMetricEvent().getFields().put(MetricTagConstants.TRIGGER_COUNT, silence.getTriggerCount());
        value.getMetricEvent().getFields().put(MetricTagConstants.SILENCE, silence.silence);

        silence.setTrigger(value.getTrigger());
        silence.setLastTimestamp(ctx.timestamp());
        silence.setLastAlertLevel(value.getLevel());

        state.update(silence);
        out.collect(value);
    }

    @Data
    public static class SilenceState {

        private long lastTimestamp;

        private AlertLevel lastAlertLevel;

        private AlertTrigger trigger;

        private long triggerCount;

        private long silence;

        private long silenceCount;
    }
}
