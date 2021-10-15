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
import cloud.erda.analyzer.common.functions.MetricEventCorrectFunction;
import cloud.erda.analyzer.common.models.MetricEvent;
import cloud.erda.analyzer.common.schemas.MetricEventSchema;
import cloud.erda.analyzer.common.schemas.MetricEventSerializeFunction;
import cloud.erda.analyzer.common.schemas.StringMetricEventSchema;
import cloud.erda.analyzer.common.utils.ExecutionEnv;
import cloud.erda.analyzer.common.watermarks.BoundedOutOfOrdernessWatermarkGenerator;
import cloud.erda.analyzer.common.watermarks.MetricWatermarkExtractor;
import cloud.erda.analyzer.runtime.MetricRuntime;
import cloud.erda.analyzer.runtime.functions.MetricEventSelectFunction;
import cloud.erda.analyzer.runtime.functions.MetricSelectOutputProcessFunction;
import cloud.erda.analyzer.runtime.models.AggregatedMetricEvent;
import cloud.erda.analyzer.runtime.models.Expression;
import cloud.erda.analyzer.runtime.models.ExpressionFunction;
import cloud.erda.analyzer.runtime.models.ExpressionMetadata;
import cloud.erda.analyzer.runtime.sources.FlinkMysqlAppendSource;
import cloud.erda.analyzer.runtime.sources.MetricExpressionMetadataReader;
import cloud.erda.analyzer.runtime.utils.OutputTagUtils;
import cloud.erda.analyzer.tracing.functions.*;
import cloud.erda.analyzer.tracing.model.Span;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
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
        env.getConfig().registerTypeWithKryoSerializer(Expression.class, CompatibleFieldSerializer.class);
        env.getConfig().registerTypeWithKryoSerializer(ExpressionFunction.class, CompatibleFieldSerializer.class);
        env.getConfig().setAutoWatermarkInterval(Time.seconds(10).toMilliseconds());

        DataStream<Span> spanStream = env.addSource(new FlinkKafkaConsumer<>(
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

        DataStream<MetricEvent> serviceStream = spanStream
                .filter(new SpanServiceTagCheckFunction())
                .name("check whether the tag of the service exists")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .keyBy(new SpanServiceGroupFunction())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new SpanServiceReduceFunction())
                .name("reduce span service")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .map(new SpanToServiceMetricFunction())
                .name("map reduced span to service metric")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR));

        DataStream<MetricEvent> tranMetricStream = spanStream
                .keyBy(Span::getTraceID)
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .process(new TransactionAnalysisFunction())
                .name("trace analysis windows process")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .flatMap(new SlowOrErrorMetricFunction(parameterTool))
                .name("slow or error metric process")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .keyBy(new MetricTagGroupFunction())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new MetricFieldAggregateFunction())
                .name("Aggregate metrics field process")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR));

        DataStream<MetricEvent> tracingMetrics = serviceStream.union(tranMetricStream)
                .flatMap(new MetricMetaFunction())
                .name("add metric meta")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OUTPUT));

        tracingMetrics
                .flatMap(new MetricEventSerializeFunction())
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OUTPUT))
                .addSink(new FlinkKafkaProducer<>(
                        parameterTool.getRequired(Constants.KAFKA_BROKERS),
                        parameterTool.getRequired(Constants.TOPIC_TRACING_METRICS),
                        new StringMetricEventSchema()))
                .name("send trace metrics to kafka")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OUTPUT));

        //规则表达式数据
        DataStream<ExpressionMetadata> metricExpressionQuery = env
                .addSource(new FlinkMysqlAppendSource<>(Constants.METRIC_EXPRESSION_QUERY, parameterTool.getLong(Constants.METRIC_METADATA_INTERVAL, 180000), new MetricExpressionMetadataReader(), parameterTool.getProperties()))
                .forceNonParallel()
                .returns(ExpressionMetadata.class)
                .name("Query metric expression from mysql");

        // metrics from kafka
        DataStream<MetricEvent> spotSpan = env
                .addSource(new FlinkKafkaConsumer<>(parameterTool.getRequired(Constants.TOPIC_SPOT_TRACE), new MetricEventSchema(), parameterTool.getProperties()))
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_INPUT))
                .name("spot span consumer")
                .flatMap(new MetricEventCorrectFunction())
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_INPUT))
                .name("Filter spot span not null and correct timestamp")
                .assignTimestampsAndWatermarks(new MetricWatermarkExtractor())
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_INPUT))
                .name("spot span consumer watermark");

        DataStream<MetricEvent> spanMetrics = spanStream.map(new SpanMetricCompatibleFunction())
                .name("map oap-span to spot span")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR));

        DataStream<AggregatedMetricEvent> aggregationMetricEvent = MetricRuntime.run(spanMetrics.union(spotSpan).union(tracingMetrics), metricExpressionQuery, parameterTool);


        SingleOutputStreamOperator<AggregatedMetricEvent> outputMetrics = aggregationMetricEvent
                .process(new MetricSelectOutputProcessFunction(OutputTagUtils.OutputMetricTag, null, null))
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

        log.info(env.getExecutionPlan());
        env.execute();
    }
}
