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

package cloud.erda.analyzer.common.watermarks;

import cloud.erda.analyzer.common.models.MetricEvent;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class MetricWatermarkExtractor extends BoundedOutOfOrdernessTimestampExtractor<MetricEvent> {

    public MetricWatermarkExtractor() {
        super(Time.seconds(10));
    }

    @Override
    public long extractTimestamp(MetricEvent element) {
        return element.getTimestamp() / (1000 * 1000) - 1;
    }

//    @Nullable
//    @Override
//    public Watermark getCurrentWatermark() {
//        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
//    }
//
//    @Override
//    public long extractTimestamp(MetricEvent metricEvent, long l) {
//        long timestamp = metricEvent.getTimestamp() / (1000 * 1000);
//        if (timestamp > currentTimestamp) {
//            this.currentTimestamp = timestamp;
//        }
//        return currentTimestamp;
//    }
}
