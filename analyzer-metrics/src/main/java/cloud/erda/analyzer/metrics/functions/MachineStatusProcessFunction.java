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

package cloud.erda.analyzer.metrics.functions;

import cloud.erda.analyzer.common.constant.MetricConstants;
import cloud.erda.analyzer.common.constant.MetricFieldConstants;
import cloud.erda.analyzer.common.constant.MetricTagConstants;
import cloud.erda.analyzer.common.models.MetricEvent;
import cloud.erda.analyzer.common.utils.StringUtil;
import cloud.erda.analyzer.metrics.model.MachineStatus;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * @author: liuhaoyang
 * @create: 2019-12-23 18:06
 **/
@Slf4j
public class MachineStatusProcessFunction extends KeyedProcessFunction<String, MetricEvent, MetricEvent> {

    private ValueStateDescriptor<MachineStatus> machineStatusValueStateDescriptor;
    private static final long delay = 1000 * 30;
    private Map<String, Boolean> timers = new HashMap<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        TypeInformation<MachineStatus> typeInformation = TypeInformation.of(MachineStatus.class);
        machineStatusValueStateDescriptor = new ValueStateDescriptor<MachineStatus>("machine-status-state", typeInformation);
    }

    @Override
    public void processElement(MetricEvent value, Context ctx, Collector<MetricEvent> out) throws Exception {
        ValueState<MachineStatus> machineStatus = getRuntimeContext().getState(machineStatusValueStateDescriptor);
        MachineStatus lastMachineStatus = machineStatus.value();
        if (lastMachineStatus == null) {
            lastMachineStatus = new MachineStatus();
            lastMachineStatus.setTimestamp(0);
        }

        Map<String, String> tags = value.getTags();
        String id = tags.get(MetricTagConstants.CLUSTER_NAME) + "/" + tags.get(MetricTagConstants.HOSTIP);
        lastMachineStatus.setId(id);
        lastMachineStatus.setClusterName(tags.get(MetricTagConstants.CLUSTER_NAME));
        lastMachineStatus.setHost(tags.get(MetricTagConstants.HOST));
        lastMachineStatus.setHostIp(tags.get(MetricTagConstants.HOSTIP));
        lastMachineStatus.set_meta(tags.get(MetricTagConstants.META));
        lastMachineStatus.set_metric_scope(tags.get(MetricTagConstants.METRIC_SCOPE));
        lastMachineStatus.set_metric_scope_id(tags.get(MetricTagConstants.METRIC_SCOPE_ID));
        if (tags.containsKey(MetricTagConstants.LABELS)) {
            lastMachineStatus.setLabels(tags.get(MetricTagConstants.LABELS));
        }

        List<String> labels = this.getLabels(value);
        if (labels.contains(MetricTagConstants.LABELS_OFFLINE)) {
            lastMachineStatus.setOffline(true);
        }

        Boolean timer = timers.computeIfAbsent(id, h -> false);

        if (!timer) {
            long next = ctx.timestamp() + delay;
            ctx.timerService().registerProcessingTimeTimer(next);
            timers.put(id, true);
        }

        /**
         * 只允许10分钟的延迟
         */
        long eventTime = value.getTimestamp() / 1000000;
        if (ctx.timestamp() - eventTime > delay * 20) {
            return;
        }
        /**
         * 忽略乱序的数据
         */
        if (eventTime < lastMachineStatus.getTimestamp()) {
            eventTime = lastMachineStatus.getTimestamp();
        }

        lastMachineStatus.setTimestamp(eventTime);
        lastMachineStatus.setStatus(MetricFieldConstants.READY);
        lastMachineStatus.setProcessingTime(ctx.timestamp());
        machineStatus.update(lastMachineStatus);
        out.collect(toEvent(lastMachineStatus, ctx.timestamp()));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<MetricEvent> out) throws Exception {
        ValueState<MachineStatus> machineStatusValueState = getRuntimeContext().getState(machineStatusValueStateDescriptor);
        MachineStatus lastMachineStatus = machineStatusValueState.value();
        if (lastMachineStatus == null) {
            return;
        }
        if (lastMachineStatus.isOffline()) {
            return;
        }

        if (ctx.timestamp() - lastMachineStatus.getProcessingTime() > (delay * 2)) {
            lastMachineStatus.setStatus(MetricFieldConstants.NOT_READY);
            out.collect(toEvent(lastMachineStatus, ctx.timestamp()));
        }

        ctx.timerService().registerProcessingTimeTimer(ctx.timestamp() + delay);

//        lastMachineStatus.setProcessingTime(ctx.timestamp());
        machineStatusValueState.update(lastMachineStatus);
    }

    private MetricEvent toEvent(MachineStatus machineStatus, long timestamp) {
        MetricEvent metricEvent = new MetricEvent();
        metricEvent.setTimestamp(timestamp * 1000 * 1000);
        metricEvent.setName(MetricConstants.MACHINE_STATUS);
        metricEvent.getTags().put(MetricTagConstants.CLUSTER_NAME, machineStatus.getClusterName());
        metricEvent.getTags().put(MetricTagConstants.HOST, machineStatus.getHost());
        metricEvent.getTags().put(MetricTagConstants.HOSTIP, machineStatus.getHostIp());
        metricEvent.getTags().put(MetricTagConstants.LABELS, machineStatus.getLabels());
        metricEvent.getTags().put(MetricTagConstants.META, machineStatus.get_meta());
        metricEvent.getTags().put(MetricTagConstants.METRIC_SCOPE, machineStatus.get_metric_scope());
        metricEvent.getTags().put(MetricTagConstants.METRIC_SCOPE_ID, machineStatus.get_metric_scope_id());

        metricEvent.getFields().put(MetricFieldConstants.STATUS, machineStatus.getStatus());
        metricEvent.getFields().put(MetricFieldConstants.LAST_TIMESTAMP, machineStatus.getTimestamp());
        return metricEvent;
    }

    private List<String> getLabels(MetricEvent value) {
        if (value == null) {
            return Collections.emptyList();
        }

        String labels = value.getTags().get(MetricTagConstants.LABELS);
        if (StringUtil.isEmpty(labels)) {
            return Collections.emptyList();
        }
        return Arrays.asList(labels.split(","));
    }
}
