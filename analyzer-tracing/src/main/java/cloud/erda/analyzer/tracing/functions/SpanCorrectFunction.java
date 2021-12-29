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

import cloud.erda.analyzer.common.constant.SpanConstants;
import cloud.erda.analyzer.common.utils.MapUtils;
import cloud.erda.analyzer.tracing.model.Span;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author liuhaoyang
 * @date 2021/9/17 22:13
 */
public class SpanCorrectFunction implements FlatMapFunction<Span, Span> {

    @Override
    public void flatMap(Span span, Collector<Span> collector) throws Exception {
        if (span != null && span.getAttributes() != null) {
            if (!span.getAttributes().containsKey(SpanConstants.SPAN_LAYER)) {
                span.getAttributes().put(SpanConstants.SPAN_LAYER, getSpanLayer(span));
            }
            collector.collect(span);
        }
    }

    private String getSpanLayer(Span span) {

        if (span.getAttributes().containsKey(SpanConstants.SPAN_LAYER)) {
            return span.getAttributes().get(SpanConstants.SPAN_LAYER);
        }

        if (MapUtils.containsAnyKeys(span.getAttributes(), SpanConstants.TAG_HTTP_PATH, SpanConstants.TAG_HTTP_URL, SpanConstants.TAG_HTTP_TARGET)) {
            return SpanConstants.SPAN_LAYER_HTTP;
        }

        if (MapUtils.containsAnyKeys(span.getAttributes(), SpanConstants.TAG_RPC_TARGET, SpanConstants.TAG_RPC_SERVICE, SpanConstants.TAG_RPC_METHOD, SpanConstants.TAG_DUBBO_SERVICE, SpanConstants.TAG_DUBBO_METHOD)) {
            return SpanConstants.SPAN_LAYER_RPC;
        }

        if (MapUtils.containsAnyKeys(span.getAttributes(), SpanConstants.MESSAGE_BUS_DESTINATION)) {
            return SpanConstants.SPAN_LAYER_MQ;
        }

        if (MapUtils.containsAnyKeys(span.getAttributes(), SpanConstants.DB_STATEMENT)) {
            String dbType = MapUtils.getByAnyKeys(span.getAttributes(), SpanConstants.DB_SYSTEM, SpanConstants.DB_TYPE);
            if (dbType != null) {
                if (SpanConstants.DB_TYPE_REDIS.equalsIgnoreCase(dbType)) {
                    return SpanConstants.SPAN_LAYER_CACHE;
                }
                return SpanConstants.SPAN_LAYER_DB;
            }
        }

        return SpanConstants.SPAN_LAYER_LOCAL;
    }
}
