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

import cloud.erda.analyzer.common.constant.LogConstant;
import cloud.erda.analyzer.common.models.LogEvent;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LogEventSchema implements SerializationSchema<LogEvent>, DeserializationSchema<LogEvent> {

    private static final Logger logger = LoggerFactory.getLogger(LogEventSchema.class);

    private static final String DEFAULT_LOG_STREAM = "stdout";
    private static final String DEFAULT_LOG_LEVEL = "INFO";

    private static final Gson GSON = new Gson();

    private String[] idKeys;

    public LogEventSchema(String idKeys) {
        this.idKeys = idKeys.split(",");
    }

    @Override
    public LogEvent deserialize(byte[] bytes) {
        LogEvent logEvent;
        try {
            logEvent = GSON.fromJson(new String(bytes), LogEvent.class);

            Map<String, String> tags = logEvent.getTags();
            if (tags == null) {
                tags = Maps.newHashMap();
                logEvent.setTags(tags);
            }

            String level = tags.get(LogConstant.LEVEL_KEY);

            if (level == null) {
                level = DEFAULT_LOG_LEVEL;
            } else {
                level = level.toUpperCase();
            }

            tags.put(LogConstant.LEVEL_KEY, level);

            for (String idKey : idKeys) {
                String id = tags.get(idKey);
                if (id != null) {
                    logEvent.setId(id);
                    break;
                }
            }

            if (StringUtils.isBlank(logEvent.getStream())) {
                logEvent.setStream(DEFAULT_LOG_STREAM);
            }

            if (logEvent.getOffset() == null) {
                logEvent.setOffset(0L);
            }

            return logEvent;
        } catch (Exception e) {
            logger.error("Deserialize LogEvent fail", e);
            return null;
        }
    }

    private String getRequestId(Object extend) {
        if (extend == null) {
            return null;
        }

        String[] arr = extend.toString().trim().split(",");
        if (arr.length < 2) {
            return null;
        }
        return arr[1];
    }

    private void addKeysToMap(Map<String, Object> from, Map<String, String> to, String... keys) {
        for (String key : keys) {
            Object value = from.get(key);
            if (value != null) {
                to.put(key, value.toString());
            }
        }
    }

    @Override
    public boolean isEndOfStream(LogEvent logEvent) {
        return false;
    }

    @Override
    public byte[] serialize(LogEvent logEvent) {
        return GSON.toJson(logEvent).getBytes();
    }

    @Override
    public TypeInformation<LogEvent> getProducedType() {
        return TypeInformation.of(LogEvent.class);
    }
}
