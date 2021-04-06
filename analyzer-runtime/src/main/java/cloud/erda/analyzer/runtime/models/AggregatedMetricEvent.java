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

package cloud.erda.analyzer.runtime.models;

import cloud.erda.analyzer.common.models.MetricEvent;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author: liuhaoyang
 * @create: 2019-06-29 21:42
 **/
@Data
public class AggregatedMetricEvent {

    public String key;

    private String metadataId;

    private String alias;

    private MetricEvent metric;

    private Map<String, String> attributes;

    private List<AggregateResult> results;

    private Set<String> outputs;

    private boolean operatorResult;
}
