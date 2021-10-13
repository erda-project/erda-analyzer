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

package cloud.erda.analyzer.tracing.functions;

import cloud.erda.analyzer.common.models.MetricEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.FileFilter;
import java.util.Map;

/**
 * @author liuhaoyang
 * @date 2021/10/13 16:09
 */
public class SpotSpanCorrectFunction implements FilterFunction<MetricEvent> {

    private static final String FIELD_START_TIME = "start_time";
    private static final String FIELD_END_TIME = "end_time";

    private boolean fieldCorrect(Map<String, Object> fields, String... fieldNames) {
        for (String fieldName : fieldNames) {
            Object field = fields.get(fieldName);
            if (field == null) {
                return false;
            }
            if (field instanceof Number) {
                // don't need to do anything
            } else if (field instanceof String) {
                try {
                    Long longField = Long.parseLong(field.toString());
                    fields.put(fieldName, longField);
                } catch (Exception exception) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean filter(MetricEvent metricEvent) throws Exception {
        return fieldCorrect(metricEvent.getFields(), FIELD_START_TIME, FIELD_END_TIME);
    }
}
