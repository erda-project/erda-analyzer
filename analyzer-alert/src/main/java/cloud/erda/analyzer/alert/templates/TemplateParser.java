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
import java.util.*;

/**
 * @author: liuhaoyang
 * @create: 2019-09-10 11:27
 **/
public class TemplateParser implements Serializable {
    private final static String Prefix = "{{";
    private final static String Suffix = "}}";

    public Template parse(String message) {
        return parseVariables(message);
    }

    private Template parseVariables(String value) {
        int len = value.length();
        int index = 0;
        Step step = Step.Prefix;
        List<TemplateVariable> variables = new ArrayList<>();
        String message = value;
        while (index > -1 && index < len) {
            switch (step) {
                case Prefix:
                    index = value.indexOf(Prefix, index);
                    if (index != -1) {
                        step = Step.Suffix;
                    }
                    break;
                case Suffix:
                    int prefixIndex = index;
                    index = value.indexOf(Suffix, prefixIndex);
                    if (index != -1) {
                        String variable = value.substring(prefixIndex + 2, index);
                        String replaced = value.substring(prefixIndex, index + 2);
                        message = message.replace(replaced, "%s");
                        variables.add(new TemplateVariable(variables.size(), variable.trim()));
                    }
                    step = Step.Prefix;
                    break;
                default:
                    break;
            }
        }
        variables.sort(Comparator.comparingInt(v -> v.getIndex()));
        Template template = new Template();
        template.setMessage(message);
        template.setVariables(variables);
        return template;
    }

    public static enum Step {
        Prefix,
        Suffix
    }
}
