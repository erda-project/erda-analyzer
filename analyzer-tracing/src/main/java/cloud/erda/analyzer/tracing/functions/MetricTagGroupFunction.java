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
        StringBuilder sb = new StringBuilder();
        sb.append(metricEvent.getName());
        Map<String, String> sortedTags = new TreeMap<>(metricEvent.getTags());
        for (Map.Entry<String, String> tag : sortedTags.entrySet()) {
            sb.append("_").append(tag.getKey()).append("_").append(tag.getValue());
        }
        String series = sb.toString();
        String md5HexKey = DigestUtils.md5Hex(series);
        if (log.isDebugEnabled()) {
            log.debug("metric series = {} . md5HexKey = {}", series, md5HexKey);
        }
        return md5HexKey;
    }
}
