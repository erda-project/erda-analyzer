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

package cloud.erda.analyzer.alert.watermarks;

import cloud.erda.analyzer.alert.models.RenderedAlertEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author: liuhaoyang
 * @create: 2020-01-06 16:42
 **/
@Slf4j
public class RenderedAlertEventWatermarkExtractor extends BoundedOutOfOrdernessTimestampExtractor<RenderedAlertEvent> {

    private long currentTimestamp = Long.MIN_VALUE;

    public RenderedAlertEventWatermarkExtractor() {
        super(Time.seconds(10));
    }

    @Override
    public long extractTimestamp(RenderedAlertEvent element) {
        return element.getMetricEvent().getTimestamp() / (1000 * 1000) - 1;
    }
}
