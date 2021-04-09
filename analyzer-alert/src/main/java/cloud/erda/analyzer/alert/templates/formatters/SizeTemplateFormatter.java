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

import cloud.erda.analyzer.alert.utils.NumberUtils;

import java.util.HashMap;
import java.util.Map;

public class SizeTemplateFormatter implements TemplateFormatter {

    private static final String[] units= {"B","KB","MB","GB","TB","PB","EB","ZB","YB","BB","NB","DB"};
    private static final Map<String,String> unitsMap = new HashMap<String, String>() {{
        put("byte","B");
    }};
    private String unit;

    public SizeTemplateFormatter(String unit) {
        this.unit = unit;
        unit = unitsMap.get(this.unit);
        if(unit != null) {
            this.unit = unit;
        }
    }

    @Override
    public String formatter() {
        return TemplateFormatterDefine.SIZE;
    }

    @Override
    public Object format(Object value) {
        if(value instanceof Number) {
            int index = 0;
            for(int i=0; i< units.length; i++) {
                if(units[i].equalsIgnoreCase(this.unit)) {
                    index = i;
                    break;
                }
            }
            String unit = units[index];
            double size = ((Number) value).doubleValue();
            String nag = "";
            if(size < (double)0) {
                nag = "-";
                size = -size;
            }
            for(int i=index; size>=(double)1024 && (i+1) < units.length; i++) {
                size = size/1024;
                unit = units[i+1];
            }
            return nag + NumberUtils.formatDouble(size) + unit;
        }
        return value;
    }
}
