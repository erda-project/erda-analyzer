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

import cloud.erda.analyzer.alert.models.Notify;
import cloud.erda.analyzer.alert.models.NotifyEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;

@Slf4j
public class NotifyBroadcastProcessFunction extends BroadcastProcessFunction<NotifyEvent, Notify, NotifyEvent> {

    private MapStateDescriptor<Long, Notify> notifyStateDescriptor;
    private long stateTtl;
    private long lastCleanTime;

    public NotifyBroadcastProcessFunction(long stateTtl, MapStateDescriptor<Long, Notify> notifyStateDescriptor) {
        this.stateTtl = stateTtl;
        this.notifyStateDescriptor = notifyStateDescriptor;
    }

    @Override
    public void processElement(NotifyEvent notifyEvent, ReadOnlyContext readOnlyContext, Collector<NotifyEvent> collector) throws Exception {
        if (notifyEvent == null) {
            return;
        }
        ReadOnlyBroadcastState<Long, Notify> notifyMapState = readOnlyContext.getBroadcastState(notifyStateDescriptor);
        Notify notify = notifyMapState.get(notifyEvent.getNotifyId());
        if (notify != null) {
            NotifyEvent result = notifyEvent.copy();
            result.setNotify(notify);
            result.getNotify().setNotifyId(result.getTemplateId());
            result.setNotifyId(notify.getId());
            collector.collect(result);
        }
    }

    @Override
    public void processBroadcastElement(Notify notify, Context context, Collector<NotifyEvent> collector) throws Exception {
        cleanExpireState(context);
        if (notify == null) {
            return;
        }
        context.getBroadcastState(notifyStateDescriptor).put(notify.getId(), notify);
    }


    private void cleanExpireState(Context ctx) throws Exception {
        long now = System.currentTimeMillis();
        if (now - lastCleanTime < stateTtl) {
            return;
        }

        lastCleanTime = now;

        BroadcastState<Long, Notify> notifyMapState = ctx.getBroadcastState(notifyStateDescriptor);
        if (notifyMapState == null) {
            return;
        }
        Iterator<Map.Entry<Long,Notify>> item = notifyMapState.entries().iterator();
        while (item.hasNext()) {
            Map.Entry<Long,Notify> entry = item.next();
            if (now - entry.getValue().getProcessTime() > stateTtl) {
                item.remove();
            }
            log.info("Clean up expired notify metadata.");
        }
    }
}
