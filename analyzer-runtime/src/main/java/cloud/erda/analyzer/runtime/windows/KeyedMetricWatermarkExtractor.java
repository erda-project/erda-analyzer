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

package cloud.erda.analyzer.runtime.windows;

import cloud.erda.analyzer.runtime.models.KeyedMetricEvent;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author: liuhaoyang
 * @create: 2019-06-30 15:28
 **/
public class KeyedMetricWatermarkExtractor extends BoundedOutOfOrdernessTimestampExtractor<KeyedMetricEvent> {

    public KeyedMetricWatermarkExtractor() {
        super(Time.seconds(10));
    }

    @Override
    public long extractTimestamp(KeyedMetricEvent element) {
        return element.getMetric().getTimestamp() / (1000 * 1000) - 1;
    }

//    @Nullable
//    @Override
//    public Watermark getCurrentWatermark() {
//        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
//    }
//
//    @Override
//    public long extractTimestamp(KeyedMetricEvent element, long previousElementTimestamp) {
//        long timestamp = element.getMetric().getTimestamp() / 1000000;
//        this.currentTimestamp = timestamp;
//        return timestamp;
//    }
}
