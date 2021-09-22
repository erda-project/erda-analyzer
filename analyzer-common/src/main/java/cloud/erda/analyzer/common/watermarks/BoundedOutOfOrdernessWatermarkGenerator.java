/*
 * Copyright (c) 2021 Terminus, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cloud.erda.analyzer.common.watermarks;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.util.Preconditions;

import java.time.Duration;

/**
 * @author liuhaoyang
 * @date 2021/9/22 13:50
 */
public class BoundedOutOfOrdernessWatermarkGenerator<T> implements WatermarkGeneratorSupplier<T> {

    private Duration maxOutOfOrderness;

    public BoundedOutOfOrdernessWatermarkGenerator(Duration maxOutOfOrderness) {
        this.maxOutOfOrderness = maxOutOfOrderness;
    }

    @Override
    public WatermarkGenerator<T> createWatermarkGenerator(Context context) {
        return new BoundedOutOfOrdernessWatermarks(maxOutOfOrderness);
    }

    public static class BoundedOutOfOrdernessWatermarks<T> implements WatermarkGenerator<T> {

        private long maxTimestamp;
        private final long outOfOrdernessMillis;

        public BoundedOutOfOrdernessWatermarks(Duration maxOutOfOrderness) {
            Preconditions.checkNotNull(maxOutOfOrderness, "maxOutOfOrderness");
            Preconditions.checkArgument(!maxOutOfOrderness.isNegative(), "maxOutOfOrderness cannot be negative");
            this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();
            this.maxTimestamp = -9223372036854775808L + this.outOfOrdernessMillis + 1L;
        }

        public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
            this.maxTimestamp = Math.max(this.maxTimestamp, eventTimestamp);
        }

        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(this.maxTimestamp - this.outOfOrdernessMillis));
        }
    }
}
