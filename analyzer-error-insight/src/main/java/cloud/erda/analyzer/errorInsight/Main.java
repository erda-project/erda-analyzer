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

package cloud.erda.analyzer.errorInsight;

import cloud.erda.analyzer.common.constant.Constants;
import cloud.erda.analyzer.common.functions.MetricEventCorrectFunction;
import cloud.erda.analyzer.common.schemas.MetricEventSchema;
import cloud.erda.analyzer.common.utils.CassandraSinkUtils;
import cloud.erda.analyzer.common.utils.ExecutionEnv;
import cloud.erda.analyzer.common.watermarks.MetricWatermarkExtractor;
import cloud.erda.analyzer.errorInsight.functions.*;
import cloud.erda.analyzer.errorInsight.model.ErrorCountState;
import cloud.erda.analyzer.errorInsight.model.ErrorDescription;
import cloud.erda.analyzer.errorInsight.model.ErrorEvent;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import static cloud.erda.analyzer.common.constant.Constants.STREAM_PARALLELISM_INPUT;

@Slf4j
public class Main {

    public static void main(final String[] args) throws Exception {
        val parameterTool = ExecutionEnv.createParameterTool(args);
        val env = ExecutionEnv.prepare(parameterTool);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // map to error_event
        val eventStream = env.addSource(new FlinkKafkaConsumer<>(
                parameterTool.getRequired(Constants.TOPIC_ERROR),
                new MetricEventSchema(),
                parameterTool.getProperties()))
                .name("error consumer")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_INPUT))
                .flatMap(new MetricEventCorrectFunction())
                .setParallelism(parameterTool.getInt(STREAM_PARALLELISM_INPUT))
                .name("filter metric not null")
                .assignTimestampsAndWatermarks(new MetricWatermarkExtractor())
                .name("error consumer watermark")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_INPUT))
                .filter(new ErrorEventValidityFilter())
                .name("error event validity filter")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .map(new ErrorEventMapper())
                .name("error event map")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR));

        // map to request_event_mapping
        val requestMappingStream = eventStream
                .filter(new RequestMappingFilter())
                .name("request mapping filter")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .map(new RequestMappingMapper())
                .name("request mapping mapper")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR));

        // map to error_event_mapping
        val errorEventMappingStream = eventStream
                .map(new ErrorEventMappingMapper())
                .name("error event mapping")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR));

        val errorInfo = eventStream
                .filter(new ProjectFilter())
                .name("project filter")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .map(new ErrorInfoMapper())
                .name("error info mapper")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR));

        // map to error description
        val errorDescription = errorInfo
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .map(new ErrorDescriptionMapper())
                .name("error description mapper")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR));

        // calculate error count
        val errorCountStateStream = errorInfo
                .map(new ErrorCountStateMapper())
                .name("error count state mapper")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .keyBy(ErrorCountState::getErrorId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(parameterTool.getLong(Constants.STREAM_WINDOW_SECONDS, 60))))
                .reduce(new ErrorCountStateAggregator())
                .name("error count state aggregator")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR));

        // map to error_count table
        val errorCountStream = errorCountStateStream.map(new ErrorCountMapper())
                .name("error count mapper")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR));

        // send error_count_metrics to kafka
        errorCountStateStream.map(new ErrorCountMetricMapper())
                .name("error count metric mapper")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .addSink(new FlinkKafkaProducer<>(
                        parameterTool.getRequired(Constants.KAFKA_BROKERS),
                        parameterTool.getRequired(Constants.TOPIC_METRICS),
                        new MetricEventSchema()))
                .name("error-count-metrics")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR));

        errorInfo.map(new ErrorAlertMapper())
                .name("error alert mapper")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .addSink(new FlinkKafkaProducer<>(
                        parameterTool.getRequired(Constants.KAFKA_BROKERS),
                        parameterTool.getRequired(Constants.TOPIC_METRICS),
                        new MetricEventSchema()))
                .name("error-alert-metrics")
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR));

        if (parameterTool.getBoolean(Constants.WRITE_EVENT_TO_ES_ENABLE)) {
            eventStream.map(ErrorEvent::toString).setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                    .addSink(new FlinkKafkaProducer<String>(
                            parameterTool.getRequired(Constants.KAFKA_BROKERS),
                            parameterTool.getRequired(Constants.TOPIC_ERROR_EVENT),
                            new SimpleStringSchema()))
                    .name("error-event-es-sink")
                    .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR));

            errorDescription.map(ErrorDescription::toString).setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                    .addSink(new FlinkKafkaProducer<String>(
                            parameterTool.getRequired(Constants.KAFKA_BROKERS),
                            parameterTool.getRequired(Constants.TOPIC_ERROR_DESCRIPTION),
                            new SimpleStringSchema()))
                    .name("error-description-es-sink")
                    .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR));
        } else {
            CassandraSinkUtils.addSink(eventStream, env, parameterTool);
            CassandraSinkUtils.addSink(requestMappingStream, env, parameterTool);
            CassandraSinkUtils.addSink(errorEventMappingStream, env, parameterTool);
            CassandraSinkUtils.addSink(errorDescription, env, parameterTool);
            CassandraSinkUtils.addSink(errorCountStream, env, parameterTool);
        }

        log.info(env.getExecutionPlan());

        env.execute("spot error-insight");
    }
}
