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
import java.util.HashMap;
import java.util.Map;

@Data
public class MetricEvent {

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
}
