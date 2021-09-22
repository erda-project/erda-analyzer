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

package cloud.erda.analyzer.tracing.functions;

import cloud.erda.analyzer.common.utils.StringUtil;
import cloud.erda.analyzer.tracing.model.Span;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author liuhaoyang
 * @date 2021/9/19 21:38
 */
public class TraceTimeGapExtractor implements SessionWindowTimeGapExtractor<Span> {

    private final Long rootSpanGap;
    private final Long childSpanGap;

    public TraceTimeGapExtractor(Long rootSpanGap, Long childSpanGap) {
        this.rootSpanGap = rootSpanGap;
        this.childSpanGap = childSpanGap;
    }

    @Override
    public long extract(Span span) {
        if (StringUtil.isNotEmpty(span.getParentSpanID())) {
            return childSpanGap;
        }
        return rootSpanGap;
    }
}
