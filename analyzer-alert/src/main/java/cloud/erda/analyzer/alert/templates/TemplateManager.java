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

package cloud.erda.analyzer.alert.templates;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: liuhaoyang
 * @create: 2020-01-14 11:09
 **/
public class TemplateManager implements Serializable {
    private Map<String, TemplateRenderer> templates = new HashMap<>();

    public TemplateRenderer getRenderer(String templateName, String templateMessage, boolean variable) {
        // 在自定义告警的场景下，模板是可变的，不需要缓存
        if (variable) {
            return createTemplateRenderer(templateMessage);
        }
        return templates.computeIfAbsent(templateName, name -> createTemplateRenderer(templateMessage));
    }

    private TemplateRenderer createTemplateRenderer(String templateMessage) {
        Template template = new TemplateParser().parse(templateMessage);
        TemplateRenderer renderer = new TemplateRenderer();
        renderer.setTemplate(template);
        return renderer;
    }
}
