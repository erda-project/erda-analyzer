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

import cloud.erda.analyzer.common.constant.SpanConstants;
import cloud.erda.analyzer.common.utils.StringBuilderUtils;
import cloud.erda.analyzer.tracing.model.Span;
import org.apache.flink.api.java.functions.KeySelector;

import java.util.Map;

/**
 * @author liuhaoyang
 * @date 2021/9/21 01:00
 */
public class SpanServiceGroupFunction implements KeySelector<Span, String> {

    @Override
    public String getKey(Span span) throws Exception {
        Map<String, String> attributes = span.getAttributes();
        StringBuilder sb = StringBuilderUtils.getCachedStringBuilder();
        sb.append(attributes.get(SpanConstants.ENV_ID));
        sb.append(attributes.get(SpanConstants.SERVICE_ID));
        sb.append(attributes.get(SpanConstants.SERVICE_INSTANCE_ID));
        return sb.toString();
    }
}
