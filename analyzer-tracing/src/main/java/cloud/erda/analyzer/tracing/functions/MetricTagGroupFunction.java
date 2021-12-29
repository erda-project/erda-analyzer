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
import cloud.erda.analyzer.common.models.MetricEvent;
import cloud.erda.analyzer.common.utils.MapUtils;
import cloud.erda.analyzer.common.utils.StringBuilderUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.java.functions.KeySelector;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author liuhaoyang
 * @date 2021/9/22 02:00
 */
@Slf4j
public class MetricTagGroupFunction implements KeySelector<MetricEvent, String> {
    @Override
    public String getKey(MetricEvent metricEvent) throws Exception {
        StringBuilder sb = StringBuilderUtils.getCachedStringBuilder();
        sb.append(metricEvent.getName()).append("_");

        sb.append(metricEvent.getTags().get(SpanConstants.ENV_ID)).append("_");
        sb.append(metricEvent.getTags().get(SpanConstants.SERVICE_ID)).append("_");
        sb.append(metricEvent.getTags().get(SpanConstants.SERVICE_INSTANCE_ID)).append("_");

        String spanLayer = metricEvent.getTags().getOrDefault(SpanConstants.SPAN_LAYER, SpanConstants.SPAN_LAYER_UNKNOWN);
        sb.append(spanLayer).append("_");

        switch (spanLayer) {
            case SpanConstants.SPAN_LAYER_HTTP:
                sb.append(MapUtils.getByAnyKeys(metricEvent.getTags(), SpanConstants.TAG_HTTP_PATH, SpanConstants.TAG_HTTP_TARGET, SpanConstants.TAG_HTTP_URL));
                break;
            case SpanConstants.SPAN_LAYER_RPC:
                sb.append(MapUtils.getByAnyKeys(metricEvent.getTags(), SpanConstants.TAG_RPC_TARGET));
                break;
            case SpanConstants.APPLICATION_CACHE:
            case SpanConstants.SPAN_LAYER_DB:
                sb.append(MapUtils.getByAnyKeys(metricEvent.getTags(), SpanConstants.DB_SYSTEM, SpanConstants.DB_TYPE)).append(MapUtils.getByAnyKeys(metricEvent.getTags(), SpanConstants.DB_STATEMENT));
                break;
            case SpanConstants.SPAN_LAYER_MQ:
                sb.append(MapUtils.getByAnyKeys(metricEvent.getTags(), SpanConstants.MESSAGE_BUS_DESTINATION));
                break;
            default:
                sb.append(metricEvent.getTags().get(SpanConstants.OPERATION_NAME));
                break;
        }

        String series = sb.toString();
//        String md5HexKey = DigestUtils.md5Hex(series);
        if (log.isDebugEnabled()) {
            log.debug("FieldAggregator group metric series = {}   md5HexKey = {}", series, "");
        }
        return series;
    }
}
