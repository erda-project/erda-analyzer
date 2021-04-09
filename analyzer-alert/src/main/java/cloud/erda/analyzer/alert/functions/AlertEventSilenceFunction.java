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
import cloud.erda.analyzer.alert.models.AlertTrigger;
import cloud.erda.analyzer.common.constant.Constants;
import cloud.erda.analyzer.common.constant.MetricTagConstants;
import lombok.Data;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import static cloud.erda.analyzer.common.constant.MetricTagConstants.TRIGGER_DURATION;

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
        ValueState<SilenceState> state = getRuntimeContext().getState(silenceStateDescriptor);
        SilenceState silence = state.value();
        if (silence == null) {
            silence = new SilenceState();
            silence.setLastTimestamp(0);
            silence.setTrigger(AlertTrigger.alert);
            silence.setCount(0);
            silence.setEventCount(0);
            silence.setSilence(value.getAlertNotify().getSilence());
            state.update(silence);
        }

        if (silence.getTrigger().equals(value.getTrigger())) {
            if (ctx.timestamp() - silence.getLastTimestamp() < silence.getSilence()) {
                return;
            }
        }

        // set silence with doubled policy
        if (value.getAlertNotify().getSilencePolicy().equals(Constants.DoubledSilencePolicy)) {
            if (value.getAlertNotify().getSilence() * Math.pow(2, silence.eventCount) < Constants.MaxSilence) {
                silence.setSilence((long) (value.getAlertNotify().getSilence() * Math.pow(2, silence.eventCount)));
                silence.setEventCount(silence.getEventCount() + 1);
            } else {
                silence.setSilence(Constants.MaxSilence);
            }
        } else {
            silence.setSilence(value.getAlertNotify().getSilence());
        }

        value.getMetricEvent().getFields().put(MetricTagConstants.TRIGGER_COUNT, silence.getCount());
        value.getMetricEvent().getFields().put(MetricTagConstants.SILENCE, silence.silence);

        // reset eventCount and silence when alert recover
        if (value.getMetricEvent().getFields().containsKey(TRIGGER_DURATION)) {
            silence.setEventCount(0);
            silence.setSilence(value.getAlertNotify().getSilence());
        }

        silence.setTrigger(value.getTrigger());
        silence.setLastTimestamp(ctx.timestamp());
        silence.setCount(0);
        out.collect(value);
        state.update(silence);
    }

    @Data
    public static class SilenceState {

        private long lastTimestamp;

        private AlertTrigger trigger;

        private long count;

        private long silence;

        private long eventCount;
    }
}
