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

import cloud.erda.analyzer.alert.models.*;
import cloud.erda.analyzer.common.constant.AlertConstants;
import cloud.erda.analyzer.common.models.MetricEvent;
import cloud.erda.analyzer.common.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class AlertEventSuppressBroadcastProcessFunction extends BroadcastProcessFunction<MetricEvent, AlertEventSuppress, MetricEvent> {

    private MapStateDescriptor<String, AlertEventSuppress> alertEventSuppressStateDescriptor;

    public AlertEventSuppressBroadcastProcessFunction(MapStateDescriptor<String, AlertEventSuppress> alertEventSuppressStateDescriptor) {
        this.alertEventSuppressStateDescriptor = alertEventSuppressStateDescriptor;
    }

    @Override
    public void processElement(MetricEvent value, ReadOnlyContext ctx, Collector<MetricEvent> out) throws Exception {
        if (value == null) {
            return;
        }

        ReadOnlyBroadcastState<String, AlertEventSuppress> templateState = ctx.getBroadcastState(alertEventSuppressStateDescriptor);
        boolean suppressed = false;

        String eventFamilyId = value.getTags().get(AlertConstants.ALERT_EVENT_FAMILY_ID);
        if (StringUtil.isNotEmpty(eventFamilyId)) {
            AlertEventSuppress suppressSetting = templateState.get(eventFamilyId);
            if (suppressSetting != null && suppressSetting.isEnabled() && (suppressSetting.getSuppressType() == AlertSuppressType.Terminate
                    || suppressSetting.getSuppressType() == AlertSuppressType.TimerRecover && suppressSetting.getExpireTime().getTime() > System.currentTimeMillis())) {
                suppressed = true;
            }
        }

        value.getTags().put(AlertConstants.ALERT_SUPPRESSED, Boolean.toString(suppressed));
        out.collect(value);
    }

    @Override
    public void processBroadcastElement(AlertEventSuppress value, Context ctx, Collector<MetricEvent> out) throws Exception {
        if (value == null) {
            return;
        }
        ctx.getBroadcastState(alertEventSuppressStateDescriptor).put(value.getAlertEventId(), value);
    }
}
