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
import cloud.erda.analyzer.common.models.SpanEvent;
import cloud.erda.analyzer.common.utils.JsonMapperUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;

public class SpanEventSchema implements DeserializationSchema<SpanEvent>, SerializationSchema<SpanEvent> {

    private final static Logger logger = LoggerFactory.getLogger(SpanEventSchema.class);

    @Override
    public SpanEvent deserialize(byte[] bytes) throws IOException {
        try {
            MetricEvent metric = JsonMapperUtils.toObject(bytes, MetricEvent.class);
            if (metric.getName() == null || metric.getName().length() <= 0) return null;
            SpanEvent span = new SpanEvent();
            span.setTags(metric.getTags());
            span.setTraceId(metric.getTags().get("trace_id"));
            span.setSpanId(metric.getTags().get("span_id"));
            span.setParentSpanId(metric.getTags().get("parent_span_id"));
            span.setOperationName(metric.getTags().get("operation_name"));
            span.setStartTime(new BigDecimal(String.valueOf(metric.getFields().get("start_time"))).longValue());
            span.setEndTime(new BigDecimal(String.valueOf(metric.getFields().get("end_time"))).longValue());
            return span;
        } catch (Exception e) {
            logger.error("Deserialize SpanEvent fail ", e);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(SpanEvent spanEvent) {
        return false;
    }

    @Override
    public TypeInformation<SpanEvent> getProducedType() {
        return TypeInformation.of(SpanEvent.class);
    }

    @Override
    public byte[] serialize(SpanEvent spanEvent) {
        try {
            return JsonMapperUtils.toBytes(spanEvent);
        } catch (Throwable e) {
            logger.error("Serialize SpanEvent fail ", e);
            return null;
        }
    }
}
