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

package cloud.erda.analyzer.alert.templates;

import cloud.erda.analyzer.common.models.MetricEvent;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: liuhaoyang
 * @create: 2019-09-10 15:10
 **/
public class TemplateContext {
    public static Map<String, Object> fromMetric(MetricEvent metricEvent) {
        Map<String, Object> templateContext = new HashMap<>(metricEvent.getTags());
        metricEvent.getFields().forEach(templateContext::put);
        return templateContext;
    }
}
