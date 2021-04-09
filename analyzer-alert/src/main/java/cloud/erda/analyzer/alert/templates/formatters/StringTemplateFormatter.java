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

package cloud.erda.analyzer.alert.templates.formatters;

import cloud.erda.analyzer.common.utils.StringUtil;

/**
 * @author: liuhaoyang
 * @create: 2020-02-03 12:11
 **/
public class StringTemplateFormatter implements TemplateFormatter {

    private int length = 0;

    public StringTemplateFormatter(String unit) {
        if (!StringUtil.isEmpty(unit)) {
            try {
                length = Integer.parseInt(unit);
            } catch (Exception ignore) {
            }
        }
    }

    @Override
    public String formatter() {
        return TemplateFormatterDefine.STRING;
    }

    @Override
    public Object format(Object value) {
        if (value instanceof String) {
            if (length > 0) {
                return ((String) value).substring(0, length);
            }
        }
        return value;
    }
}
