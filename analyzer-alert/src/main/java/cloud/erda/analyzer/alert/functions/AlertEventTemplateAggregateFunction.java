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
import scala.collection.convert.Wrappers;

import java.util.Iterator;

/**
 * @author: liuhaoyang
 * @create: 2020-01-06 00:53
 **/
@Slf4j
public class AlertEventTemplateAggregateFunction extends ProcessWindowFunction<RenderedAlertEvent, RenderedAlertEvent, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<RenderedAlertEvent> elements, Collector<RenderedAlertEvent> out) throws Exception {
        // TODO 广发使用外部API (WEBHOOK) 方式进行告警，不进行聚合。这里先对 WEBHOOK 的告警简单的特殊处理，后面要优化重构掉。
        RenderedAlertEvent result = null;
        RenderedAlertEvent renderedAlertEvent;
        int dingLength = 20000;
        Iterator<RenderedAlertEvent> iterator = elements.iterator();
        if (iterator.hasNext()) {
            renderedAlertEvent = iterator.next();
            if (renderedAlertEvent.getTemplateTarget().equals(AlertConstants.ALERT_TEMPLATE_TARGET_WEBHOOK)) {
                out.collect(renderedAlertEvent);
                while (iterator.hasNext()) {
                    renderedAlertEvent = iterator.next();
                    out.collect(renderedAlertEvent);
                }
            } else {
                String content = renderedAlertEvent.getContent();
                while (iterator.hasNext()) {
                    renderedAlertEvent = iterator.next();
                    if ((content + "\n\n&nbsp;\n\n" + renderedAlertEvent.getContent()).length() > dingLength) {
                        setResult(result,renderedAlertEvent,content);
                        content = renderedAlertEvent.getContent();
                        out.collect(result);
                    } else {
                        content = content + "\n\n&nbsp;\n\n" + renderedAlertEvent.getContent();
                    }
                }
                setResult(result,renderedAlertEvent,content);
                out.collect(result);
            }
        }
    }
    public void setResult (RenderedAlertEvent result,RenderedAlertEvent renderedAlertEvent,String content) {
        result.setContent(content);
        result.setId(renderedAlertEvent.getId());
        result.setTitle(renderedAlertEvent.getTitle());
        result.setMetricEvent(renderedAlertEvent.getMetricEvent());
        result.setNotifyTarget(renderedAlertEvent.getNotifyTarget());
        result.setTemplateTarget(renderedAlertEvent.getTemplateTarget());
    }
}
