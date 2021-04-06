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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: liuhaoyang
 * @create: 2019-09-10 11:03
 **/
@Slf4j
public class TemplateLoader {
    private static final ConcurrentHashMap<String, Template> templates = new ConcurrentHashMap<>();

    public static TemplateRenderer load(String templateName) throws Exception {
        Template template = templates.computeIfAbsent(templateName, TemplateLoader::loadTemplate);
        if (template == null) {
            throw new RuntimeException("Cannot load template. id = " + templateName);
        }
        TemplateRenderer renderer = new TemplateRenderer();
        renderer.setTemplate(template);
        return renderer;
    }

    private static Template loadTemplate(String name) {
        try {
            try (InputStream templateStream = TemplateLoader.class.getClassLoader().getResourceAsStream("template/" + name + ".txt")) {
                String message = readToString(templateStream);
                return new TemplateParser().parse(message);
            }
        } catch (IOException ex) {
            log.error("Cannot load template. id = " + name, ex);
            return null;
        }
    }

    private static String readToString(InputStream inputStream) throws IOException {
        try (StringWriter writer = new StringWriter()) {
            IOUtils.copy(inputStream, writer, StandardCharsets.UTF_8.name());
            return writer.toString();
        }
    }
}
