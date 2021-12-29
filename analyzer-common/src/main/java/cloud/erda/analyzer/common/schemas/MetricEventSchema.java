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
import cloud.erda.analyzer.common.utils.JsonMapperUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@Slf4j
public class MetricEventSchema implements DeserializationSchema<MetricEvent>, SerializationSchema<MetricEvent> {

    private final static Logger logger = LoggerFactory.getLogger(MetricEventSchema.class);

    @Override
    public MetricEvent deserialize(byte[] bytes) throws IOException {
        try {
            return JsonMapperUtils.toObject(bytes, MetricEvent.class);
        } catch (Throwable throwable) {
            logger.error("Deserialize metric event fail. \nSource : {} \n", new String(bytes), throwable);
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
            return JsonMapperUtils.toBytes(metricEvent);
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
