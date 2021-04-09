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

package cloud.erda.analyzer.runtime.utils;

import cloud.erda.analyzer.runtime.models.AggregatedMetricEvent;
import cloud.erda.analyzer.runtime.models.KeyedMetricEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;

/**
 * @author liuhaoyang
 * @date 2021/1/28 13:03
 */
public class OutputTagUtils {
    public static final OutputTag<KeyedMetricEvent> AllowedLatenessTag =
            new OutputTag<KeyedMetricEvent>("window-behavior-allowed-lateness", TypeInformation.of(KeyedMetricEvent.class));

    public static final OutputTag<AggregatedMetricEvent> OutputMetricTag =
            new OutputTag<AggregatedMetricEvent>("output-metric", TypeInformation.of(AggregatedMetricEvent.class));

    public static final OutputTag<AggregatedMetricEvent> OutputMetricTempTag =
            new OutputTag<AggregatedMetricEvent>("output-metric-temp", TypeInformation.of(AggregatedMetricEvent.class));

    public static final OutputTag<AggregatedMetricEvent> OutputAlertEventTag =
            new OutputTag<AggregatedMetricEvent>("output-alert", TypeInformation.of(AggregatedMetricEvent.class));

}
