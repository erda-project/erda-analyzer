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
import cloud.erda.analyzer.alert.models.AlertNotify;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: liuhaoyang
 * @create: 2020-01-05 20:19
 **/
@Slf4j
public class AlertNotifyBroadcastProcessFunction extends BroadcastProcessFunction<AlertEvent, AlertNotify, AlertEvent> {

    private MapStateDescriptor<String, Map<Long, AlertNotify>> alertNotifyStateDescriptor;
    private long stateTtl;
    private long lastCleanTime;

    public AlertNotifyBroadcastProcessFunction(long stateTtl, MapStateDescriptor<String, Map<Long, AlertNotify>> alertNotifyStateDescriptor) {
        this.alertNotifyStateDescriptor = alertNotifyStateDescriptor;
        this.stateTtl = stateTtl;
    }

    @Override
    public void processElement(AlertEvent value, ReadOnlyContext ctx, Collector<AlertEvent> out) throws Exception {
        if (value == null) {
            return;
        }
        ReadOnlyBroadcastState<String, Map<Long, AlertNotify>> notifyMapState = ctx.getBroadcastState(alertNotifyStateDescriptor);
        Map<Long, AlertNotify> notifies = notifyMapState.get(value.getAlertId());
        if (notifies != null && !notifies.isEmpty()) {
            for (AlertNotify notify : notifies.values()) {
                if (notify.isEnable()) {
                    AlertEvent result = value.copy();
                    result.setAlertNotify(notify);
                    out.collect(result);
                }
            }
        }
    }

    @Override
    public void processBroadcastElement(AlertNotify value, Context ctx, Collector<AlertEvent> out) throws Exception {
        if (value == null) {
            return;
        }

        cleanExpireState(ctx);

        BroadcastState<String, Map<Long, AlertNotify>> notifyMapState = ctx.getBroadcastState(alertNotifyStateDescriptor);
        Map<Long, AlertNotify> items = notifyMapState.get(value.getAlertId());
        if (items == null) {
            items = new HashMap<>();
            notifyMapState.put(value.getAlertId(), items);
        }
        items.put(value.getId(), value);
    }

    private void cleanExpireState(Context ctx) throws Exception {
        long now = System.currentTimeMillis();
        if (now - lastCleanTime < stateTtl) {
            return;
        }

        lastCleanTime = now;

        BroadcastState<String, Map<Long, AlertNotify>> notifyMapState = ctx.getBroadcastState(alertNotifyStateDescriptor);
        if (notifyMapState == null) {
            return;
        }
        for (Map.Entry<String, Map<Long, AlertNotify>> item : notifyMapState.entries()) {
            for (Map.Entry<Long, AlertNotify> val : new HashMap<Long, AlertNotify>(item.getValue()).entrySet())
                if (now - val.getValue().getProcessingTime() > stateTtl) {
                    item.getValue().remove(val.getKey());
                }
        }

        log.info("Clean up expired notify metadata.");
    }
}
