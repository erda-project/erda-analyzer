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

package cloud.erda.analyzer.common.models;

import lombok.Data;
import lombok.val;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
public class MetricEvent implements Serializable {

    // measurement name
    private String name;

    // timestamp for metric
    private long timestamp;

    // values for metric
    private Map<String, Object> fields = new HashMap<>();

    // tags for metric
    private Map<String, String> tags = new HashMap<>();

    // copy a metric
    public MetricEvent copy() {
        val metric = new MetricEvent();
        metric.name = this.name;
        metric.timestamp = this.timestamp;
        metric.fields = this.fields;
        metric.tags = this.tags;
        return metric;
    }

    // add tag whit ingore empty key and val
    public MetricEvent addTag(String key, String val) {
        if (key == null || "".equals(key)) return this;
        if (val == null || "".equals(val)) return this;
        tags.put(key, val);
        return this;
    }

    public MetricEvent addField(String key, Object val) {
        if (key == null || "".equals(key)) return this;
        if (val == null) return this;
        fields.put(key, val);
        return this;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(timestamp).append(" [Name=").append(name).append("] Tags[");
        for (Map.Entry<String, String> tag : tags.entrySet()) {
            sb.append(tag.getKey()).append("=").append(tag.getValue()).append(" ");
        }
        sb.append("] Fields[");
        for (Map.Entry<String, Object> field : fields.entrySet()) {
            sb.append(field.getKey()).append("=").append(field.getValue().toString()).append(" ");
        }
        sb.append("]");
        return sb.toString();
    }

    public String getTag(String... keys) {
        for (String key : keys) {
            String val = tags.get(key);
            if (val != null) {
                return val;
            }
        }
        return null;
    }
}
