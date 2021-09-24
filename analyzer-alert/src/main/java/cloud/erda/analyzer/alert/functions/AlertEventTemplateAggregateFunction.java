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

package cloud.erda.analyzer.alert.functions;

import cloud.erda.analyzer.alert.models.RenderedAlertEvent;
import cloud.erda.analyzer.common.constant.AlertConstants;
import cloud.erda.analyzer.common.utils.StringUtil;
import com.sun.org.apache.xpath.internal.axes.PredicatedNodeTest;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNested;
import scala.collection.convert.Wrappers;

import java.util.Iterator;

/**
 * @author: liuhaoyang
 * @create: 2020-01-06 00:53
 **/
@Slf4j
public class AlertEventTemplateAggregateFunction extends ProcessWindowFunction<RenderedAlertEvent, RenderedAlertEvent, String, TimeWindow> {
    private static final String space = "\n\n&nbsp;\n\n";

    @Override
    public void process(String s, Context context, Iterable<RenderedAlertEvent> elements, Collector<RenderedAlertEvent> out) throws Exception {
        RenderedAlertEvent result = new RenderedAlertEvent();
        RenderedAlertEvent renderedAlertEvent = new RenderedAlertEvent();
        int dingLength = 20000;
        if (elements.iterator().hasNext()) {
            renderedAlertEvent = elements.iterator().next();
        }
        if (renderedAlertEvent.getTemplateTarget().equals(AlertConstants.ALERT_TEMPLATE_TARGET_WEBHOOK)) {
            for (RenderedAlertEvent element : elements) {
                out.collect(element);
            }
        } else {
            String content = "";
            for (RenderedAlertEvent element : elements) {
                renderedAlertEvent = element;
                if ((content + space + renderedAlertEvent.getContent()).getBytes("utf-8").length > dingLength) {
                    if (content.equals("")) {
                        continue;
                    }
                    setResult(result, renderedAlertEvent, content);
                    out.collect(result);
                    content = "";
                }
                content += space + renderedAlertEvent.getContent();
            }
            if (!content.equals("")) {
                setResult(result, renderedAlertEvent, content);
                out.collect(result);
            }
        }
    }

    public void setResult(RenderedAlertEvent result, RenderedAlertEvent renderedAlertEvent, String content) {
        result.setContent(content);
        result.setId(renderedAlertEvent.getId());
        result.setTitle(renderedAlertEvent.getTitle());
        result.setMetricEvent(renderedAlertEvent.getMetricEvent());
        result.setNotifyTarget(renderedAlertEvent.getNotifyTarget());
        result.setTemplateTarget(renderedAlertEvent.getTemplateTarget());
    }
}
