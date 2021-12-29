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

import cloud.erda.analyzer.common.schemas.MetricEventSerializeFunction;
import cloud.erda.analyzer.common.schemas.StringMetricEventSchema;
import cloud.erda.analyzer.metrics.functions.*;
import cloud.erda.analyzer.runtime.sources.AllAlertExpressions;
import cloud.erda.analyzer.runtime.sources.AllMetricExpressions;
import cloud.erda.analyzer.runtime.MetricRuntime;
import cloud.erda.analyzer.runtime.functions.MetricAlertSelectFunction;
import cloud.erda.analyzer.runtime.functions.MetricEventSelectFunction;
import cloud.erda.analyzer.runtime.functions.MetricSelectOutputProcessFunction;
import cloud.erda.analyzer.runtime.models.*;
import cloud.erda.analyzer.runtime.utils.OutputTagUtils;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import cloud.erda.analyzer.common.constant.Constants;
import cloud.erda.analyzer.common.functions.MetricEventCorrectFunction;
import cloud.erda.analyzer.common.models.MetricEvent;
import cloud.erda.analyzer.common.schemas.MetricEventSchema;
import cloud.erda.analyzer.common.utils.ExecutionEnv;
import cloud.erda.analyzer.common.watermarks.MetricWatermarkExtractor;
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

@Slf4j
public class Main {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnv.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnv.prepare(parameterTool);
        env.getConfig().registerTypeWithKryoSerializer(Expression.class, CompatibleFieldSerializer.class);
        env.getConfig().registerTypeWithKryoSerializer(ExpressionFunction.class, CompatibleFieldSerializer.class);
        //从monitor中alert表达式数据
        DataStream<ExpressionMetadata> allAlertExpressions = env.addSource(new AllAlertExpressions(parameterTool.get(Constants.MONITOR_ADDR)))
                .forceNonParallel()
                .returns(ExpressionMetadata.class)
                .name("get alert expressions from monitor");

        //从monitor中获取metric表达式数据
        DataStream<ExpressionMetadata> allMetricExpressions = env.addSource(new AllMetricExpressions(parameterTool.get(Constants.MONITOR_ADDR)))
                .forceNonParallel()
                .returns(ExpressionMetadata.class)
                .name("get metric expressions from monitor");

        DataStream<ExpressionMetadata> expressionQuery = allAlertExpressions.union(allMetricExpressions);

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


        DataStream<AggregatedMetricEvent> aggregationMetricEvent = MetricRuntime.run(metrics.union(machineMetrics), expressionQuery, parameterTool);


        SingleOutputStreamOperator<AggregatedMetricEvent> outputMetrics = aggregationMetricEvent
                .process(new MetricSelectOutputProcessFunction(OutputTagUtils.OutputMetricTag, OutputTagUtils.OutputMetricTempTag, OutputTagUtils.OutputAlertEventTag))
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .name("Split outputs");

        // metric output
        outputMetrics.getSideOutput(OutputTagUtils.OutputMetricTag)
                .flatMap(new MetricEventSelectFunction())
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .name("Map metric output to metricEvent")
                .flatMap(new MetricEventSerializeFunction())
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OUTPUT))
                .addSink(new FlinkKafkaProducer<>(
                        parameterTool.getRequired(Constants.KAFKA_BROKERS),
                        parameterTool.getRequired(Constants.TOPIC_METRICS),
                        new StringMetricEventSchema()))
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OUTPUT))
                .name("Push metric output to kafka");

        // metric view output
        outputMetrics.getSideOutput(OutputTagUtils.OutputMetricTempTag)
                .flatMap(new MetricEventSelectFunction())
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .name("Map metric temp output to metricEvent")
                .flatMap(new MetricEventSerializeFunction())
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OUTPUT))
                .addSink(new FlinkKafkaProducer<>(
                        parameterTool.getRequired(Constants.KAFKA_BROKERS),
                        parameterTool.getRequired(Constants.TOPIC_METRICS_TEMP),
                        new StringMetricEventSchema()))
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OUTPUT))
                .name("Push metric temp output to kafka");

        // alert output
        outputMetrics.getSideOutput(OutputTagUtils.OutputAlertEventTag)
                .keyBy(AggregatedMetricEvent::getKey)
                .process(new MetricAlertSelectFunction())
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .name("Process alert recover_status and map metric to alert.")
                .flatMap(new MetricEventSerializeFunction())
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OUTPUT))
                .addSink(new FlinkKafkaProducer<>(
                        parameterTool.getRequired(Constants.KAFKA_BROKERS),
                        parameterTool.getRequired(Constants.TOPIC_ALERT),
                        new StringMetricEventSchema()))
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OUTPUT))
                .name("Push alert output to kafka");

        log.info("Execution plan : {}", env.getExecutionPlan());

        env.execute("Dice metrics aggregation engine.");
    }
}
