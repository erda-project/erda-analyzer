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

import cloud.erda.analyzer.alert.models.NotifyEvent;
import cloud.erda.analyzer.alert.models.UniversalTemplate;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class NotifyTemplateProcessFunction extends BroadcastProcessFunction<NotifyEvent, UniversalTemplate, NotifyEvent> {
    private MapStateDescriptor<String, Map<String,UniversalTemplate>> notifyTemplateMapStateDescriptor;
    private long stateTtl;
    private long lastCleanTime;

    public NotifyTemplateProcessFunction(long stateTtl, MapStateDescriptor<String, Map<String,UniversalTemplate>> notifyTemplateMapStateDescriptor) {
        this.stateTtl = stateTtl;
        this.notifyTemplateMapStateDescriptor = notifyTemplateMapStateDescriptor;
    }

    @Override
    public void processElement(NotifyEvent notifyEvent, ReadOnlyContext readOnlyContext, Collector<NotifyEvent> collector) throws Exception {
        if (notifyEvent == null) {
            return;
        }
        ReadOnlyBroadcastState<String, Map<String,UniversalTemplate>> templateState = readOnlyContext.getBroadcastState(notifyTemplateMapStateDescriptor);
        //这里不用遍历直接获取templateid
        Map<String,UniversalTemplate> template = templateState.get(notifyEvent.getTemplateId());
        if (template != null && !template.isEmpty()) {
            for (String chan : notifyEvent.getNotify().getTarget().getChannels()) {
                UniversalTemplate t = template.get(chan);
                if (t != null) {
                    NotifyEvent event = notifyEvent.copy();
                    event.setNotifyTemplate(t);
                    collector.collect(event);
                }
            }
        }
    }

    @Override
    public void processBroadcastElement(UniversalTemplate notifyTemplate, Context context, Collector<NotifyEvent> collector) throws Exception {
        if (notifyTemplate == null) {
            return;
        }
        cleanExpireState(context);
        BroadcastState<String,Map<String,UniversalTemplate>> notifyTemplateState= context.getBroadcastState(notifyTemplateMapStateDescriptor);
        Map<String,UniversalTemplate> items = notifyTemplateState.get(notifyTemplate.getNotifyId());
        if (items == null) {
            items = new HashMap<>();
            notifyTemplateState.put(notifyTemplate.getNotifyId(),items);
        }
        items.put(notifyTemplate.getTemplate().getTarget(),notifyTemplate);
    }

    private void cleanExpireState(Context ctx) throws Exception {
        long now = System.currentTimeMillis();
        if (now - lastCleanTime < stateTtl) {
            return;
        }
        lastCleanTime = now;
        BroadcastState<String, Map<String,UniversalTemplate>> templates = ctx.getBroadcastState(notifyTemplateMapStateDescriptor);
        if (templates == null) {
            return;
        }
//        Map<String, UniversalTemplate> immutableEntries = new HashMap<>();
//        templates.immutableEntries().forEach(entry -> immutableEntries.put(entry.getKey(), entry.getValue()));
        for(Map.Entry<String,Map<String,UniversalTemplate>> item : templates.entries()) {
            for (Map.Entry<String,UniversalTemplate> val : new HashMap<>(item.getValue()).entrySet()){
                if (now - val.getValue().getProcessingTime() > stateTtl) {
                    item.getValue().remove(val.getKey());
                }
            }
        }
//        for (Map.Entry<String, UniversalTemplate> template : immutableEntries.entrySet()) {
//            if (now - template.getValue().getProcessingTime() > stateTtl) {
//                templates.remove(template.getKey());
//            }
//        }
        log.info("clean up expired notify template");
    }
}
