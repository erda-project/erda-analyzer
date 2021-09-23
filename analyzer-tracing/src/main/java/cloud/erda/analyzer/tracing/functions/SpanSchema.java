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

import cloud.erda.analyzer.tracing.model.Span;
import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author liuhaoyang
 * @date 2021/9/17 17:12
 */
public class SpanSchema implements DeserializationSchema<Span> {

    private final static Logger logger = LoggerFactory.getLogger(SpanSchema.class);
    private final static Gson gson = new Gson();

    @Override
    public Span deserialize(byte[] bytes) throws IOException {
        try {
            Span span = gson.fromJson(new String(bytes), Span.class);
            return span;
        } catch (Exception e) {
            logger.error("Deserialize SpanEvent fail ", e);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(Span span) {
        return false;
    }

    @Override
    public TypeInformation<Span> getProducedType() {
        return TypeInformation.of(Span.class);
    }
}
