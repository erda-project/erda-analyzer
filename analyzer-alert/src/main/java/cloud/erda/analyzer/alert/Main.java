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

package cloud.erda.analyzer.alert;

import cloud.erda.analyzer.alert.functions.*;
import cloud.erda.analyzer.alert.models.*;
import cloud.erda.analyzer.alert.sources.*;
import cloud.erda.analyzer.alert.watermarks.RenderedAlertEventWatermarkExtractor;
import cloud.erda.analyzer.alert.sinks.EventBoxSink;
import cloud.erda.analyzer.alert.utils.StateDescriptors;
import cloud.erda.analyzer.alert.watermarks.AlertEventWatermarkExtractor;
import cloud.erda.analyzer.common.constant.AlertConstants;
import cloud.erda.analyzer.common.constant.Constants;
import cloud.erda.analyzer.common.functions.MetricEventCorrectFunction;
import cloud.erda.analyzer.common.models.Event;
import cloud.erda.analyzer.common.models.MetricEvent;
import cloud.erda.analyzer.common.schemas.MetricEventSchema;
import cloud.erda.analyzer.common.utils.CassandraSinkUtils;
import cloud.erda.analyzer.common.utils.ExecutionEnv;
import cloud.erda.analyzer.common.watermarks.MetricWatermarkExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import schemas.RecordSchema;

import static cloud.erda.analyzer.common.constant.Constants.*;

@Slf4j
public class Main {

    /*
     * 1、读Kafka：alert - metric-event
     * 2、读Mysql：alert_notify、alert_notify_template
     * 3、数据转换，metric-event => alert-event
     * 4、alert_notify、alert_notify_template，广播到alert-event
     * 4、ticket 、history 直接发工单
     * 5、dingding、notify_group 进行消息静默
     * 6、静默后的dingding、notify_group 消息聚合
     * 7、聚合后 dingding、notify_group 发送到eventbox
     * */

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnv.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnv.prepare(parameterTool);

        DataStream<MetricEvent> alertMetric = env.addSource(new FlinkKafkaConsumer<>(
                parameterTool.getRequired(Constants.TOPIC_ALERT),
                new MetricEventSchema(),
                parameterTool.getProperties()))
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_INPUT))
                .flatMap(new MetricEventCorrectFunction())
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_INPUT))
                .assignTimestampsAndWatermarks(new MetricWatermarkExtractor())
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_INPUT))
                .name("alert metrics consumer");

        //获取notify相关的metric
        DataStream<MetricEvent> notifyMetric = env.addSource(new FlinkKafkaConsumer<>(
                parameterTool.getRequired(Constants.TOPIC_NOTIFY),
                new MetricEventSchema(),
                parameterTool.getProperties()))
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_INPUT))
                .flatMap(new MetricEventCorrectFunction())
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_INPUT))
                .assignTimestampsAndWatermarks(new MetricWatermarkExtractor())
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_INPUT))
                .name("alert metrics consumer");

        // 存储原始告警数据
        alertMetric.addSink(new FlinkKafkaProducer<>(
                parameterTool.getRequired(Constants.KAFKA_BROKERS),
                parameterTool.getRequired(Constants.TOPIC_METRICS),
                new MetricEventSchema()
        )).setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OUTPUT))
                .name("Store raw alert metrics to kafka");

        notifyMetric.addSink(new FlinkKafkaProducer<>(
                parameterTool.getRequired(Constants.KAFKA_BROKERS),
                parameterTool.getRequired(Constants.TOPIC_METRICS),
                new MetricEventSchema()
        )).setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OUTPUT))
                .name("Store raw notify metrics to kafka");

        DataStream<AlertNotify> alertNotifies = env.addSource(new AlertNotifyReader(parameterTool.get(Constants.MONITOR_ADDR)))
                .forceNonParallel()
                .returns(AlertNotify.class)
                .name("Query notify from monitor");

        DataStream<AlertNotifyTemplate> alertNotifyTemplate = env.addSource(new NotifyAlertTemplateReader(parameterTool.get(Constants.MONITOR_ADDR)))
                .forceNonParallel()
                .returns(AlertNotifyTemplate.class)
                .name("get alert templates from monitor");
        //获取对应组织设置的语言
        DataStream<Org> orgLocale = env.addSource(new OrgLocaleReader(parameterTool.get(Constants.MONITOR_ADDR)))
                .forceNonParallel()
                .returns(Org.class)
                .name("get org locale from monitor");


        // metric转换event
        DataStream<AlertEvent> alertEvents = alertMetric
                .flatMap(new AlertEventMapFunction())
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .name("map metric to alert event");

        DataStream<AlertEvent> alertEventsWithNotify = alertEvents
                .connect(alertNotifies.broadcast(StateDescriptors.alertNotifyStateDescriptor))
                .process(new AlertNotifyBroadcastProcessFunction(parameterTool.getLong(METRIC_METADATA_TTL, 75000), StateDescriptors.alertNotifyStateDescriptor))
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .name("broadcast alert notify");

        DataStream<AlertEvent> alertEventsWithLocale = alertEventsWithNotify
                .connect(orgLocale.broadcast(StateDescriptors.orgLocaleStateDescriptor))
                .process(new OrgLocaleBroadcastProcessFunction(parameterTool.getLong(METRIC_METADATA_TTL, 75000), StateDescriptors.orgLocaleStateDescriptor))
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .name("broadcast org locale");

//        DataStream<AlertEvent> alertEventsWithTemplate = alertEventsWithNotify
        DataStream<AlertEvent> alertEventsWithTemplate = alertEventsWithLocale
                .connect(alertNotifyTemplate.broadcast(StateDescriptors.alertNotifyTemplateStateDescriptor))
                .process(new AlertNotifyTemplateBroadcastProcessFunction(parameterTool.getLong(METRIC_METADATA_TTL,
                        75000), StateDescriptors.alertNotifyTemplateStateDescriptor))
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .name("broadcast alert notify template");

        // ticket告警事件和渲染
        DataStream<RenderedAlertEvent> ticketAlertRender = alertEventsWithTemplate
                .filter(new AlertEventTargetFilterFunction(AlertConstants.ALERT_NOTIFY_TYPE_TICKET))
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .map(new AlertEventTemplateRenderFunction())
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR));

        // 存储告警记录
        //不进行数据存储操作，将数据发送到kafka中，由monitor读取再存入mysql中
        ticketAlertRender.
                map(new AlertRecordMapFunction())
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .name("RenderedAlertEvent to record")
                .addSink(new FlinkKafkaProducer<>(
                        parameterTool.getRequired(Constants.KAFKA_BROKERS),
                        parameterTool.getRequired(Constants.TOPIC_RECORD_ALERT),
                        new RecordSchema(AlertRecord.class)))
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OUTPUT))
                .name("push alert record output to kafka");

        // 存储ticket告警指标
//        ticketAlertEvents
//            .map(new AlertMetricMapFunction())
//            .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
//            .addSink(new FlinkKafkaProducer011<>(
//                parameterTool.getRequired(Constants.KAFKA_BROKERS),
//                parameterTool.getRequired(Constants.TOPIC_METRICS),
//                new MetricEventSchema()
//        )).setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OUTPUT))
//                .name("Store ticket alert metrics to kafka");

        // 存储告警历史
        if (parameterTool.getBoolean(WRITE_EVENT_TO_ES_ENABLE)) {
            // 数据发送到 kafka，由 streaming 消费写入 ES
            ticketAlertRender.map(new ErdaEventMapFunction())
                    .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                    .name("RenderedAlertEvent to history")
                    .addSink(new FlinkKafkaProducer<>(
                            parameterTool.getRequired(KAFKA_BROKERS),
                            parameterTool.getRequired(TOPIC_ALERT_HISTORY),
                            new RecordSchema<>(Event.class)))
                    .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OUTPUT))
                    .name("push alert history output to kafka");
        } else {
            DataStream<AlertHistory> alertHistories = ticketAlertRender.map(new AlertHistoryMapFunction())
                    .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR));
            CassandraSinkUtils.addSink(alertHistories, env, parameterTool);
        }

        DataStream<AlertEvent> alertEventLevel = alertEventsWithTemplate
                .filter(new AlertEventLevelFilterFunction())
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .name("filter event notify level")
                .assignTimestampsAndWatermarks(new AlertEventWatermarkExtractor())
                .keyBy(new AlertEventGroupFunction())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .process(new AlertLevelProcessFunction())
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .name("processing alert level merge");

        // 告警静默
        DataStream<AlertEvent> alertEventsSilence = alertEventLevel
                .assignTimestampsAndWatermarks(new AlertEventWatermarkExtractor())
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .keyBy(new AlertEventGroupFunction())
                .process(new AlertEventSilenceFunction())
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .name("silence alert");

        // ticket, history 不收敛聚合
        //DataStream<RenderedAlertEvent> alertEventsDirect = alertEventsSilence
        //        .filter(new AlertEventTargetFilterFunction(AlertConstants.ALERT_NOTIFY_TYPE_TICKET, AlertConstants.ALERT_NOTIFY_TYPE_HISTORY))
        //        .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
        //        .map(new AlertEventTemplateRenderFunction())
        //        .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
        //        .name("direct renderer");
        //alertEventsDirect
        //        .map(new AlertTargetToTicketMapFunction())
        //        .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
        //        .addSink(new cloud.erda.analyzer.alert.sinks.TicketSink(parameterTool.getProperties()))
        //        .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OUTPUT))
        //        .name("send alert message to ticket");

        // dingding 和 notify_group 的消息1分钟内收敛聚合
        DataStream<RenderedAlertEvent> aggregatedAlertEvents = alertEventsSilence
                .filter(new AlertEventTargetFilterFunction(AlertConstants.ALERT_NOTIFY_TYPE_DINGDING, AlertConstants.ALERT_NOTIFY_TYPE_NOTIFY_GROUP))
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .map(new AlertEventTemplateRenderFunction())
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .assignTimestampsAndWatermarks(new RenderedAlertEventWatermarkExtractor())
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .keyBy(new AlertEventTemplateGroupFunction())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(45)))
                .process(new AlertEventTemplateAggregateFunction())
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .name("aggregate renderer");

        aggregatedAlertEvents
                .map(new AlertTargetToEventBoxMapFunction())
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .addSink(new EventBoxSink(parameterTool.getProperties()))
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OUTPUT))
                .name("send alert message to eventbox");

        log.info(env.getExecutionPlan());

        env.execute("spot analyzer-alert");
    }
}