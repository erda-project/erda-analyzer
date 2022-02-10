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
import cloud.erda.analyzer.alert.utils.OutputTagUtils;
import cloud.erda.analyzer.alert.watermarks.RenderedAlertEventWatermarkExtractor;
import cloud.erda.analyzer.alert.sinks.EventBoxSink;
import cloud.erda.analyzer.alert.utils.StateDescriptors;
import cloud.erda.analyzer.alert.watermarks.AlertEventWatermarkExtractor;
import cloud.erda.analyzer.common.constant.AlertConstants;
import cloud.erda.analyzer.common.constant.Constants;
import cloud.erda.analyzer.common.functions.MetricEventCorrectFunction;
import cloud.erda.analyzer.common.models.Event;
import cloud.erda.analyzer.common.models.MetricEvent;
import cloud.erda.analyzer.alert.partitioners.AlertRecordKafkaPartitioner;
import cloud.erda.analyzer.common.schemas.CommonSchema;
import cloud.erda.analyzer.common.schemas.MetricEventSchema;
import cloud.erda.analyzer.common.utils.CassandraSinkUtils;
import cloud.erda.analyzer.common.utils.ExecutionEnv;
import cloud.erda.analyzer.common.watermarks.MetricWatermarkExtractor;
import cloud.erda.analyzer.runtime.sources.FlinkMysqlAppendSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import schemas.RecordSchema;

import java.util.Optional;
import java.util.Properties;

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

        //sp_alert_event_suppress
        DataStream<AlertEventSuppress> alertEventSuppressSettings = env
                .addSource(new FlinkMysqlAppendSource<>(ALERT_EVENT_SUPPRESS_QUERY,
                        parameterTool.getLong(METRIC_METADATA_INTERVAL, 60000),
                        new AlertEventSuppressReader(), parameterTool.getProperties()))
                .forceNonParallel() // 避免多个线程重复读取mysql
                .returns(AlertEventSuppress.class)
                .name("Query alert event suppress settings from mysql");

        DataStream<MetricEvent> alertMetricWithSuppressSettings = alertMetric
                .connect(alertEventSuppressSettings.broadcast(StateDescriptors.alertEventSuppressSettingsStateDescriptor))
                .process(new AlertEventSuppressBroadcastProcessFunction(StateDescriptors.alertEventSuppressSettingsStateDescriptor))
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .name("broadcast alert event suppress settings");

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
        alertMetricWithSuppressSettings.addSink(new FlinkKafkaProducer<>(
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

        DataStream<Notify> notifyQuery = env
                .addSource(new FlinkMysqlAppendSource<>(NOTIFY_QUERY, parameterTool.getLong(METRIC_METADATA_INTERVAL, 60000),
                        new SpotNotifyReader(), parameterTool.getProperties()))
                .forceNonParallel()
                .returns(Notify.class)
                .name("Query notify from mysql");
        //sp_alert_notify
        DataStream<AlertNotify> alertNotifyQuery = env
                .addSource(new FlinkMysqlAppendSource<>(Constants.ALERT_NOTIFY_QUERY,
                        parameterTool.getLong(METRIC_METADATA_INTERVAL, 60000),
                        new NotifyReader(), parameterTool.getProperties()))
                .forceNonParallel() // 避免多个线程重复读取mysql
                .returns(AlertNotify.class)
                .name("Query alert notify from mysql");


        DataStream<NotifyTemplate> allTemplates = env.addSource(new AllNotifyTemplates(parameterTool.get(Constants.MONITOR_ADDR)))
                .forceNonParallel()
                .returns(NotifyTemplate.class)
                .name("get templates use http");

        DataStream<UniversalTemplate> allUniversalTemplates = allTemplates.flatMap(new UniversalTemplateProcessFunction())
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .name("transform to universalTemplate");

        DataStream<AlertNotifyTemplate> alertNotifyTemplateQuery = env
                .addSource(new FlinkMysqlAppendSource<>(Constants.ALERT_NOTIFY_TEMPLATE_QUERY, parameterTool.getLong(METRIC_METADATA_INTERVAL, 60000), new NotifyTemplateReader(false), parameterTool.getProperties()))
                .forceNonParallel()
                .returns(AlertNotifyTemplate.class)
                .name("Query alert notify template from mysql");
        DataStream<AlertNotifyTemplate> alertNotifyCustomTemplateQuery = env
                .addSource(new FlinkMysqlAppendSource<>(ALERT_NOTIFY_CUSTOM_TEMPLATE_QUERY, parameterTool.getLong(METRIC_METADATA_INTERVAL, 60000), new NotifyTemplateReader(true), parameterTool.getProperties()))
                .forceNonParallel()
                .returns(AlertNotifyTemplate.class)
                .name("Query alert notify custom template from mysql");

        // metric转换event
        DataStream<AlertEvent> alertEvents = alertMetricWithSuppressSettings
                .flatMap(new AlertEventMapFunction())
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .name("map metric to alert event");

        //notifyMetric转换为notify_event
        DataStream<NotifyEvent> notifyEvents = notifyMetric.flatMap(new NotifyEventMapFunction())
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .name("metric to notify event");

        //notify_events和notify_query
        DataStream<NotifyEvent> notifyEventDataStream = notifyEvents.connect(notifyQuery.broadcast(StateDescriptors.notifyStateDescriptor))
                .process(new NotifyBroadcastProcessFunction(parameterTool.getLong(METRIC_METADATA_TTL, 75000), StateDescriptors.notifyStateDescriptor))
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .name("broadcast notify");

        DataStream<AlertEvent> alertEventsWithNotify = alertEvents
                .connect(alertNotifyQuery.broadcast(StateDescriptors.alertNotifyStateDescriptor))
                .process(new AlertNotifyBroadcastProcessFunction(parameterTool.getLong(METRIC_METADATA_TTL, 75000), StateDescriptors.alertNotifyStateDescriptor))
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .name("broadcast alert notify");

        DataStream<NotifyEvent> notifyEventWithTemplate = notifyEventDataStream.connect(allUniversalTemplates.broadcast(StateDescriptors.notifyTemplate))
                .process(new NotifyTemplateProcessFunction(parameterTool.getLong(METRIC_METADATA_TTL, 7500), StateDescriptors.notifyTemplate))
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .name("notify event with template");

        DataStream<AlertEvent> alertEventsWithTemplate = alertEventsWithNotify
                .connect(alertNotifyTemplateQuery.union(alertNotifyCustomTemplateQuery).broadcast(StateDescriptors.alertNotifyTemplateStateDescriptor))
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

        //notify渲染事件
        DataStream<RenderedNotifyEvent> notifyRender = notifyEventWithTemplate
                .map(new NotifyEventTemplateRenderFunction())
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR));

        // 存储告警记录
        //不进行数据存储操作，将数据发送到kafka中，由monitor读取再存入mysql中
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", parameterTool.getRequired(Constants.KAFKA_BROKERS));
        ticketAlertRender.
                map(new AlertRecordMapFunction())
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .name("RenderedAlertEvent to record")
                .addSink(new FlinkKafkaProducer<>(
                        parameterTool.getRequired(TOPIC_RECORD_ALERT),
                        new RecordSchema<>(AlertRecord.class),
                        kafkaProps,
                        Optional.of(new AlertRecordKafkaPartitioner())))
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OUTPUT))
                .name("push alert record output to kafka");

        notifyRender.map(new NotifyRecordMapFunction())
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .name("RenderedNotifyEvent to record")
                .addSink(new FlinkKafkaProducer<>(
                        parameterTool.getRequired(KAFKA_BROKERS),
                        parameterTool.getRequired(TOPIC_RECORD_NOTIFY),
                        new RecordSchema<>(NotifyRecord.class)))
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OUTPUT))
                .name("push notify record output to kafka");

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
        if(parameterTool.getBoolean(WRITE_EVENT_TO_ES_ENABLE)){
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

        SingleOutputStreamOperator<AlertEvent> alertEventLevel = alertEventsWithTemplate
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
//        DataStream<AlertEvent> alertEventsSilence = alertEventsWithTemplate
        SingleOutputStreamOperator<AlertEvent> alertEventsSilence = alertEventLevel
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
        SingleOutputStreamOperator<RenderedAlertEvent> aggregatedAlertEvents = alertEventsSilence
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

        notifyRender.map(new NotifyTargetToEventBoxMapFunction())
                .addSink(new EventBoxSink(parameterTool.getProperties()))
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OUTPUT))
                .name("send notify message to eventbox");

        alertEventLevel.getSideOutput(OutputTagUtils.AlertEventNotifyProcess)
                .union(alertEventsSilence.getSideOutput(OutputTagUtils.AlertEventNotifyProcess))
                .union(aggregatedAlertEvents.getSideOutput(OutputTagUtils.AlertEventNotifyProcess))
                .addSink(new FlinkKafkaProducer<>(
                        parameterTool.getRequired(Constants.KAFKA_BROKERS),
                        parameterTool.getRequired(Constants.TOPIC_METRICS),
                        new CommonSchema<>(AlertEventNotifyMetric.class)))
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OUTPUT))
                .name("Store alert notify processing metrics to kafka");

        log.info(env.getExecutionPlan());

        env.execute("spot analyzer-alert");
    }
}