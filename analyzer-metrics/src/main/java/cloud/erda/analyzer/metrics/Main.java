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

package cloud.erda.analyzer.metrics;

import cloud.erda.analyzer.alert.utils.StateDescriptors;
import cloud.erda.analyzer.metrics.functions.*;
import cloud.erda.analyzer.runtime.functions.*;
import cloud.erda.analyzer.runtime.models.*;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import cloud.erda.analyzer.common.constant.Constants;
import cloud.erda.analyzer.common.functions.MetricEventCorrectFunction;
import cloud.erda.analyzer.common.models.MetricEvent;
import cloud.erda.analyzer.common.schemas.MetricEventSchema;
import cloud.erda.analyzer.common.utils.ExecutionEnv;
import cloud.erda.analyzer.common.watermarks.MetricWatermarkExtractor;
import cloud.erda.analyzer.metrics.sources.AlertExpressionMetadataReader;
import cloud.erda.analyzer.metrics.sources.MetricExpressionMetadataReader;
import cloud.erda.analyzer.runtime.sources.FlinkMysqlAppendSource;
import cloud.erda.analyzer.runtime.utils.OutputTagUtils;
import cloud.erda.analyzer.runtime.utils.StateDescriptorFactory;
import cloud.erda.analyzer.runtime.windows.KeyedMetricWatermarkExtractor;
import cloud.erda.analyzer.runtime.windows.KeyedMetricWindowAssigner;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Arrays;
import java.util.List;

import static cloud.erda.analyzer.common.constant.Constants.STREAM_PARALLELISM_OPERATOR;

@Slf4j
public class Main {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnv.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnv.prepare(parameterTool);
        env.getConfig().registerTypeWithKryoSerializer(Expression.class, CompatibleFieldSerializer.class);
        env.getConfig().registerTypeWithKryoSerializer(ExpressionFunction.class, CompatibleFieldSerializer.class);

        //查询dice_org
        DataStream<DiceOrg> diceOrgQuery = env
                .addSource(new FlinkMysqlAppendSource<>(Constants.DICE_ORG_QUERY,parameterTool.getLong(Constants.METRIC_METADATA_INTERVAL,60000),new DiceOrgReader(),parameterTool.getProperties()))
                .forceNonParallel()
                .returns(DiceOrg.class)
                .name("query dice_org form mysql");

        //规则表达式数据
        DataStream<ExpressionMetadata> alertExpressionQuery = env
                .addSource(new FlinkMysqlAppendSource<>(Constants.ALERT_EXPRESSION_QUERY, parameterTool.getLong(Constants.METRIC_METADATA_INTERVAL, 60000), new AlertExpressionMetadataReader(), parameterTool.getProperties()))
                .forceNonParallel()
                .returns(ExpressionMetadata.class)
                .name("Query alert expression from mysql");

        DataStream<ExpressionMetadata> alertExpressionOrg = alertExpressionQuery
                .connect(diceOrgQuery.broadcast(StateDescriptors.diceOrgDescriptor))
                .process(new DiceOrgBroadcastProcessFunction(StateDescriptors.diceOrgDescriptor))
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .name("alert expression with org name");

        //规则表达式数据
        DataStream<ExpressionMetadata> metricExpressionQuery = env
                .addSource(new FlinkMysqlAppendSource<>(Constants.METRIC_EXPRESSION_QUERY, parameterTool.getLong(Constants.METRIC_METADATA_INTERVAL, 60000), new MetricExpressionMetadataReader(), parameterTool.getProperties()))
                .forceNonParallel()
                .returns(ExpressionMetadata.class)
                .name("Query metric expression from mysql");

//        DataStream<ExpressionMetadata> expressionQuery = alertExpressionQuery.union(metricExpressionQuery);
        DataStream<ExpressionMetadata> expressionQuery = alertExpressionOrg.union(metricExpressionQuery);

        //metric data from kafka
        List<String> topics = Arrays.asList(parameterTool.getRequired(Constants.TOPIC_METRICS), parameterTool.getRequired(Constants.TOPIC_METRICS_TEMP));

        // metrics from kafka
        DataStream<MetricEvent> metrics = env
                .addSource(new FlinkKafkaConsumer<>(topics, new MetricEventSchema(), parameterTool.getProperties()))
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_INPUT))
                .name("Metrics consumer")
                .flatMap(new MetricEventCorrectFunction())
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_INPUT))
                .name("Filter metric not null and correct timestamp")
                .assignTimestampsAndWatermarks(new MetricWatermarkExtractor())
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_INPUT))
                .name("Metrics consumer watermark");

        // 在Task内部链接原始metrics流和machineMetrics流, 避免kafka消费延迟
        // process machine status ready or not ready
        DataStream<MetricEvent> machineMetrics = metrics.filter(new MachineFilterProcessFunction())
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
//                .assignTimestampsAndWatermarks(new MetricWatermarkExtractor())
//                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_OPERATOR))
                .name("Filter machine metric_event")
                .keyBy(new MachineHostGroupProcessFunction())
                .timeWindow(Time.seconds(60), Time.seconds(30))
                .reduce(new MachineMetricsReduceProcessFunction())
//                .process(new MachineMetricsPreProcessFunction())
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .name("Pre process machine status metric_event")
                .keyBy(new MachineHostGroupProcessFunction())
                .process(new MachineStatusProcessFunction())
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .name("Process machine status ready or not ready");

        //push host status
        machineMetrics
                .map(new Machine2HostStatusMapFunction())
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OUTPUT))
                .name("Map machine status to host status")
                .addSink(new FlinkKafkaProducer<MetricEvent>(
                        parameterTool.getRequired(Constants.KAFKA_BROKERS),
                        parameterTool.getRequired(Constants.TOPIC_METRICS),
                        new MetricEventSchema()))
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OUTPUT))
                .name("Push host_status to kafka");

        // broadcast
        DataStream<MetricEvent> filter = metrics.union(machineMetrics)
                .connect(expressionQuery.broadcast(StateDescriptorFactory.MetricFilterState))
                .process(new MetricNameFilterProcessFunction(StateDescriptorFactory.MetricFilterState))
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .name("Pre process metrics name filter");

        // broadcast
        SingleOutputStreamOperator<KeyedMetricEvent> filteredMetrics = filter
                .connect(expressionQuery.broadcast(StateDescriptorFactory.MetricKeyedMapState, StateDescriptorFactory.ExpressionState))
                .process(new MetricFilterProcessFunction(parameterTool.getLong(Constants.METRIC_METADATA_TTL, 75000), StateDescriptorFactory.MetricKeyedMapState, StateDescriptorFactory.ExpressionState))
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .name("Pre process metrics expression filters");

        SingleOutputStreamOperator<KeyedMetricEvent> exactlyNoneMetrics = filteredMetrics
                .process(new WindowBehaviorOutputProcessFunction(OutputTagUtils.AllowedLatenessTag))
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .name("Process window behavior");

        // process none lateness metrics
        SingleOutputStreamOperator<AggregatedMetricEvent> exactlyAggregationMetricEvent = exactlyNoneMetrics
                .assignTimestampsAndWatermarks(new KeyedMetricWatermarkExtractor())
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .keyBy(new MetricGroupProcessFunction())
                .window(new KeyedMetricWindowAssigner())
                .aggregate(new MetricAggregateProcessFunction(), new MetricOperatorProcessFunction())
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .name("Process expression group and functions");

        // process lateness metrics
        SingleOutputStreamOperator<AggregatedMetricEvent> lateAggregationMetricEvent = exactlyNoneMetrics
                .getSideOutput(OutputTagUtils.AllowedLatenessTag)
                .assignTimestampsAndWatermarks(new KeyedMetricWatermarkExtractor())
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .keyBy(new MetricGroupProcessFunction())
                .window(new KeyedMetricWindowAssigner()).allowedLateness(Time.minutes(3))
                .aggregate(new MetricAggregateProcessFunction(), new MetricOperatorProcessFunction())
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .name("Process expression group and functions for late-data");

        DataStream<AggregatedMetricEvent> aggregationMetricEvent = exactlyAggregationMetricEvent.union(lateAggregationMetricEvent);

        SingleOutputStreamOperator<AggregatedMetricEvent> outputMetrics = aggregationMetricEvent
                .process(new MetricSelectOutputProcessFunction(OutputTagUtils.OutputMetricTag, OutputTagUtils.OutputMetricTempTag, OutputTagUtils.OutputAlertEventTag))
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .name("Split outputs");

        // metric output
        outputMetrics.getSideOutput(OutputTagUtils.OutputMetricTag)
                .flatMap(new MetricEventSelectFunction())
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .name("Map metric output to metricEvent")
                .addSink(new FlinkKafkaProducer<>(
                        parameterTool.getRequired(Constants.KAFKA_BROKERS),
                        parameterTool.getRequired(Constants.TOPIC_METRICS),
                        new MetricEventSchema()))
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OUTPUT))
                .name("Push metric output to kafka");

        // metric view output
        outputMetrics.getSideOutput(OutputTagUtils.OutputMetricTempTag)
                .flatMap(new MetricEventSelectFunction())
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .name("Map metric temp output to metricEvent")
                .addSink(new FlinkKafkaProducer<>(
                        parameterTool.getRequired(Constants.KAFKA_BROKERS),
                        parameterTool.getRequired(Constants.TOPIC_METRICS_TEMP),
                        new MetricEventSchema()))
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OUTPUT))
                .name("Push metric temp output to kafka");

        // alert output
        outputMetrics.getSideOutput(OutputTagUtils.OutputAlertEventTag)
                .keyBy(AggregatedMetricEvent::getKey)
                .process(new MetricAlertSelectFunction())
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .name("Process alert recover_status and map metric to alert.")
                .addSink(new FlinkKafkaProducer<>(
                        parameterTool.getRequired(Constants.KAFKA_BROKERS),
                        parameterTool.getRequired(Constants.TOPIC_ALERT),
                        new MetricEventSchema()))
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OUTPUT))
                .name("Push alert output to kafka");

        log.info("Execution plan : {}", env.getExecutionPlan());

        env.execute("Dice metrics aggregation engine.");
    }
}
