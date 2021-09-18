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
import cloud.erda.analyzer.common.schemas.SpanEventSchema;
import cloud.erda.analyzer.common.utils.ExecutionEnv;
import cloud.erda.analyzer.tracing.functions.SpanCorrectFunction;
import cloud.erda.analyzer.tracing.functions.SpanMetricCompatibleMapper;
import cloud.erda.analyzer.tracing.functions.TraceAnalysisProcessFunction;
import cloud.erda.analyzer.tracing.model.Span;
import cloud.erda.analyzer.tracing.schemas.SpanSchema;
import cloud.erda.analyzer.tracing.watermarks.SpanWatermarkExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

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
        SingleOutputStreamOperator<Span> traceStream = env.addSource(new FlinkKafkaConsumer<>(
                        parameterTool.getRequired(Constants.TOPIC_OAP_TRACE),
                        new SpanSchema(),
                        parameterTool.getProperties()))
                .name("oap trace consumer")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_INPUT))
                .flatMap(new SpanCorrectFunction())
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_INPUT))
                .name("filter span not null")
                .assignTimestampsAndWatermarks(new SpanWatermarkExtractor())
                .name("span consumer watermark")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_INPUT));

        traceStream.map(new SpanMetricCompatibleMapper())
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .name("map oap-span to spot span")
                .addSink(new FlinkKafkaProducer<>(
                        parameterTool.getRequired(Constants.KAFKA_BROKERS),
                        parameterTool.getRequired(Constants.TOPIC_TRACE),
                        new MetricEventSchema()))
                .name("send spot span to kafka")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OUTPUT));

        SingleOutputStreamOperator<MetricEvent> traceMetricsStream = traceStream
                .keyBy(Span::getTraceID)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(5)))
                .process(new TraceAnalysisProcessFunction())
                .name("trace analysis windows process")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR));

        traceMetricsStream
                .addSink(new FlinkKafkaProducer<>(
                        parameterTool.getRequired(Constants.KAFKA_BROKERS),
                        parameterTool.getRequired(Constants.TOPIC_METRICS),
                        new MetricEventSchema()))
                .name("send trace metrics to kafka")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OUTPUT));

        traceMetricsStream.print();
        traceStream.print();
        log.info(env.getExecutionPlan());
        env.execute();
    }
}
