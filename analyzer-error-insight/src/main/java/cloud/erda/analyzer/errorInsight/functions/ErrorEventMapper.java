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

package cloud.erda.analyzer.errorInsight.functions;

import cloud.erda.analyzer.common.models.MetricEvent;
import cloud.erda.analyzer.errorInsight.model.ErrorEvent;
import cloud.erda.analyzer.errorInsight.utils.ErrorMD5Utils;
import lombok.val;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.HashMap;

public class ErrorEventMapper implements MapFunction<MetricEvent, ErrorEvent> {

    @Override
    public ErrorEvent map(MetricEvent metricEvent) throws Exception {
        val errorEvent = new ErrorEvent();
        errorEvent.setEventId(metricEvent.getTags().get(ErrorConstants.EVENT_ID));
        errorEvent.setRequestId(metricEvent.getTags().get(ErrorConstants.REQUEST_ID));
        errorEvent.setTimestamp(metricEvent.getTimestamp());
        val tags = new HashMap<String, String>();
        val metaData = new HashMap<String, String>();
        val requestContext = new HashMap<String, String>();
        val requestHeaders = new HashMap<String, String>();
        val stacks = new ArrayList<String>();
        for (val tag : metricEvent.getTags().entrySet()) {
            val key = tag.getKey();
            String tagValue = tag.getValue();
            if (tagValue == null) {
                continue;
            }
            if (key.startsWith(ErrorConstants.TAG_PREFIX)) {
                tags.put(key.substring(ErrorConstants.TAG_PREFIX.length()), tagValue);
            } else if (key.startsWith(ErrorConstants.METADATA_PREFIX)) {
                metaData.put(key.substring(ErrorConstants.METADATA_PREFIX.length()), tagValue);
            } else if (key.startsWith(ErrorConstants.CONTEXT_PREFIX)) {
                requestContext.put(key.substring(ErrorConstants.CONTEXT_PREFIX.length()), tagValue);
            } else if (key.startsWith(ErrorConstants.HEADER_PREFIX)) {
                requestHeaders.put(key.substring(ErrorConstants.HEADER_PREFIX.length()), tagValue);
            } else if (key.startsWith(ErrorConstants.STACK_PREFIX)) {
                stacks.add(tagValue);
            } else {
                tags.put(key, tagValue);
            }
        }
        errorEvent.setTags(tags);
        errorEvent.setMetaData(metaData);
        errorEvent.setRequestContext(requestContext);
        errorEvent.setRequestHeaders(requestHeaders);
        errorEvent.setStacks(stacks);
        errorEvent.setErrorId(ErrorMD5Utils.getMD5(errorEvent));
        return errorEvent;
    }
}
