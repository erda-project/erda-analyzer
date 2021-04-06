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

import cloud.erda.analyzer.alert.models.*;
import cloud.erda.analyzer.alert.models.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class UniversalTemplateProcessFunction implements FlatMapFunction<NotifyTemplate, UniversalTemplate> {
    @Override
    public void flatMap(NotifyTemplate notifyTemplate, Collector<UniversalTemplate> collector) throws Exception {
        for (Template template : notifyTemplate.getTemplates()) {
            for (String target : template.getTargets()) {
                UniversalTemplate universalTemplate = new UniversalTemplate();
                universalTemplate.setMetadata(new UniversalMetadata());
                universalTemplate.getMetadata().setScope(notifyTemplate.getMetadata().getScope()[0]);
                universalTemplate.setNotifyId(notifyTemplate.getNotifyId());
                universalTemplate.getMetadata().setModule(notifyTemplate.getMetadata().getModule());
                universalTemplate.getMetadata().setName(notifyTemplate.getMetadata().getName());
                universalTemplate.getMetadata().setType(notifyTemplate.getMetadata().getType());
                universalTemplate.setBehavior(notifyTemplate.getBehavior());
                universalTemplate.setTemplate(new SingleTemplate());
                universalTemplate.getTemplate().setRender(template.getRender());
                universalTemplate.getTemplate().setTrigger(template.getTrigger().toString());
                universalTemplate.getTemplate().setTarget(target);
                universalTemplate.setProcessingTime(notifyTemplate.getProcessingTime());
                collector.collect(universalTemplate);
            }
        }
    }
}
