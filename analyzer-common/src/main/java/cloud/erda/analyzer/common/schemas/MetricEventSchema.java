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

package cloud.erda.analyzer.common.schemas;

import cloud.erda.analyzer.common.models.MetricEvent;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

@Slf4j
public class MetricEventSchema implements DeserializationSchema<MetricEvent>, SerializationSchema<MetricEvent> {

    private final static Logger logger = LoggerFactory.getLogger(MetricEventSchema.class);
    private static final Gson gson = new Gson();

    @Override
    public MetricEvent deserialize(byte[] bytes) throws IOException {
        String input = new String(bytes);
        try {
            return gson.fromJson(input, MetricEvent.class);
        } catch (Throwable throwable) {
            logger.error("Deserialize metric event fail. \nSource : {} \n", input, throwable);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(MetricEvent metricEvent) {
        return false;
    }

    @Override
    public byte[] serialize(MetricEvent metricEvent) {
        try {
            return gson.toJson(metricEvent).getBytes(StandardCharsets.UTF_8);
        } catch (Exception exception) {
            logger.error("Serialize metric event fail. {}", metricEvent.toString(), exception);
            return null;
        }
    }

    @Override
    public TypeInformation<MetricEvent> getProducedType() {
        return TypeInformation.of(MetricEvent.class);
    }
}
