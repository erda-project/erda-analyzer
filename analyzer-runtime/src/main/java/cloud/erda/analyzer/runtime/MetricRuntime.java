/*
 * Copyright (c) 2021 Terminus, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cloud.erda.analyzer.runtime;

import cloud.erda.analyzer.common.constant.Constants;
import cloud.erda.analyzer.common.models.MetricEvent;
import cloud.erda.analyzer.runtime.functions.*;
import cloud.erda.analyzer.runtime.models.AggregatedMetricEvent;
import cloud.erda.analyzer.runtime.models.ExpressionMetadata;
import cloud.erda.analyzer.runtime.models.KeyedMetricEvent;
import cloud.erda.analyzer.runtime.utils.OutputTagUtils;
import cloud.erda.analyzer.runtime.utils.StateDescriptorFactory;
import cloud.erda.analyzer.runtime.windows.KeyedMetricWatermarkExtractor;
import cloud.erda.analyzer.runtime.windows.KeyedMetricWindowAssigner;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author liuhaoyang
 * @date 2021/10/8 11:38
 */
public class MetricRuntime {

    public static DataStream<AggregatedMetricEvent> run(DataStream<MetricEvent> metricStream, DataStream<ExpressionMetadata> expressionStream, ParameterTool parameterTool) {
        // broadcast
        DataStream<MetricEvent> filter =metricStream
                .connect(expressionStream.broadcast(StateDescriptorFactory.MetricFilterState))
                .process(new MetricNameFilterProcessFunction(StateDescriptorFactory.MetricFilterState))
                .setParallelism(parameterTool.getInt(Constants.STREAM_PARALLELISM_OPERATOR))
                .name("Pre process metrics name filter");

        // broadcast
        SingleOutputStreamOperator<KeyedMetricEvent> filteredMetrics = filter
                .connect(expressionStream.broadcast(StateDescriptorFactory.MetricKeyedMapState, StateDescriptorFactory.ExpressionState))
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

        return exactlyAggregationMetricEvent.union(lateAggregationMetricEvent);
    }
}
