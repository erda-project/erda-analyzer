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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.util.TimeUtils;

/**
 * @author liuhaoyang
 * @date 2021/12/15 19:31
 */
public class TraceAnalysisTimeGapExtractor implements SessionWindowTimeGapExtractor<Span> {

    private static final Long ROOT_SPAN_GAP = Time.seconds(3).toMilliseconds();

    private static final Long CHILD_SPAN_GAP = Time.seconds(15).toMilliseconds();

    @Override
    public long extract(Span span) {
        return StringUtil.isEmpty(span.getParentSpanID()) ? ROOT_SPAN_GAP : CHILD_SPAN_GAP;
    }
}
