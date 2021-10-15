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

package cloud.erda.analyzer.common.schemas;

import cloud.erda.analyzer.common.models.MetricEvent;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;

/**
 * @author liuhaoyang
 * @date 2021/10/15 17:47
 */
@Slf4j
public class MetricEventSerializeFunction implements FlatMapFunction<MetricEvent, String> {

    private static final Gson gson = new Gson();

    @Override
    public void flatMap(MetricEvent metricEvent, Collector<String> collector) throws Exception {
        try {
            collector.collect(gson.toJson(metricEvent));
        } catch (Exception exception) {
            log.error("Serialize metric event fail. {}", metricEvent.toString(), exception);
        }
    }
}
