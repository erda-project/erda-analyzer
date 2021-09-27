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

package cloud.erda.analyzer.tracing;

import cloud.erda.analyzer.common.constant.Constants;
import cloud.erda.analyzer.common.models.MetricEvent;
import cloud.erda.analyzer.common.schemas.MetricEventSchema;
import cloud.erda.analyzer.common.utils.ExecutionEnv;
import cloud.erda.analyzer.common.watermarks.BoundedOutOfOrdernessWatermarkGenerator;
import cloud.erda.analyzer.tracing.functions.*;
import cloud.erda.analyzer.tracing.model.Span;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.time.Duration;

import static cloud.erda.analyzer.common.constant.Constants.STREAM_PARALLELISM_INPUT;

/**
 * @author liuhaoyang
 * @date 2021/9/17 16:57
 */
@Slf4j
public class Main {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ExecutionEnv.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnv.prepare(parameterTool);

        env.getConfig().setAutoWatermarkInterval(Time.seconds(10).toMilliseconds());

        SingleOutputStreamOperator<Span> spanStream = env.addSource(new FlinkKafkaConsumer<>(
                        parameterTool.getRequired(Constants.TOPIC_OAP_TRACE),
                        new SpanSchema(),
                        parameterTool.getProperties()))
                .name("oap trace consumer")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_INPUT))
                .flatMap(new SpanCorrectFunction())
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_INPUT))
                .name("filter span not null")
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .forGenerator(new BoundedOutOfOrdernessWatermarkGenerator<Span>(Duration.ofSeconds(10)))
                        .withTimestampAssigner(new SpanTimestampAssigner()))
                .name("span consumer watermark")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_INPUT));

        spanStream.map(new SpanMetricCompatibleFunction())
                .name("map oap-span to spot span")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .addSink(new FlinkKafkaProducer<>(
                        parameterTool.getRequired(Constants.KAFKA_BROKERS),
                        parameterTool.getRequired(Constants.TOPIC_TRACE),
                        new MetricEventSchema()))
                .name("send spot span to kafka")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OUTPUT));

        SingleOutputStreamOperator<MetricEvent> serviceStream = spanStream
                .filter(new SpanServiceTagCheckFunction())
                .name("check whether the tag of the service exists")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .keyBy(new SpanServiceGroupFunction())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .trigger(new FixedEventTimeTrigger())
                .reduce(new SpanServiceReduceFunction())
                .name("reduce span service")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .map(new SpanToServiceMetricFunction())
                .name("map reduced span to service metric")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR));

        SingleOutputStreamOperator<MetricEvent> tranMetricStream = spanStream
                .keyBy(Span::getTraceID)
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
//                .trigger(new FixedEventTimeTrigger())
                .process(new TransactionAnalysisFunction())
                .name("trace analysis windows process")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .flatMap(new SlowOrErrorMetricFunction(parameterTool))
                .name("slow or error metric process")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .keyBy(new MetricTagGroupFunction())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .trigger(new FixedEventTimeTrigger())
                .aggregate(new MetricFieldAggregateFunction())
//                .process(new MetricFieldProcessFunction())
                .name("Aggregate metrics field process")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR));

        serviceStream.union(tranMetricStream)
                .flatMap(new MetricMetaFunction())
                .name("add metric meta")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OUTPUT))
                .addSink(new FlinkKafkaProducer<>(
                        parameterTool.getRequired(Constants.KAFKA_BROKERS),
                        parameterTool.getRequired(Constants.TOPIC_METRICS),
                        new MetricEventSchema()))
                .name("send trace metrics to kafka")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OUTPUT));

        log.info(env.getExecutionPlan());
        env.execute();
    }
}
