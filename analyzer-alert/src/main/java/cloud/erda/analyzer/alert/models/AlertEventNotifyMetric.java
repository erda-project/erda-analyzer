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

package cloud.erda.analyzer.alert.models;

import cloud.erda.analyzer.common.constant.AlertConstants;
import cloud.erda.analyzer.common.constant.MetricConstants;
import cloud.erda.analyzer.common.constant.MetricTagConstants;
import cloud.erda.analyzer.common.models.MetricEvent;
import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
public class AlertEventNotifyMetric implements Serializable {
    // measurement name
    private String name;

    // timestamp for metric
    private long timestamp;

    // values for metric
    private AlertEventNotifyMetricField fields = new AlertEventNotifyMetricField();

    // tags for metric
    private Map<String, String> tags = new HashMap<>();

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp * 1000_000;
    }

    public void addReduced() {
        fields.addReduced(1);
    }

    public void addReduced(long delta) {
        fields.addReduced(delta);
        timestamp = System.currentTimeMillis();
    }

    public void addSilenced() {
        fields.addSilenced(1);
    }

    public void addSilenced(long delta) {
        fields.addSilenced(delta);
        timestamp = System.currentTimeMillis();
    }

    public static AlertEventNotifyMetric createFrom(MetricEvent alertMetric) {
        AlertEventNotifyMetric metric = new AlertEventNotifyMetric();
        metric.setName(MetricConstants.ALERT_EVENT_NOTIFY_METRIC_NAME);
        metric.setTimestamp(System.currentTimeMillis());

        metric.getTags().put(MetricTagConstants.METRIC_SCOPE, alertMetric.getTags().get(MetricTagConstants.METRIC_SCOPE));
        metric.getTags().put(MetricTagConstants.METRIC_SCOPE_ID, alertMetric.getTags().get(MetricTagConstants.METRIC_SCOPE_ID));
        metric.getTags().put(AlertConstants.ALERT_SCOPE, alertMetric.getTags().get(AlertConstants.ALERT_SCOPE));
        metric.getTags().put(AlertConstants.ALERT_SCOPE_ID, alertMetric.getTags().get(AlertConstants.ALERT_SCOPE_ID));

        return metric;
    }
}

