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

public class TimeTemplateFormatter implements TemplateFormatter {

    private static final String[] units = {"ns","Ms","ms","s", "m","h","d"};
    private static final double[] powers =     {1,   1000,1000,1000, 60, 60,24};
    private String unit;
    public TimeTemplateFormatter(String unit) {
        this.unit = unit;
    }

    @Override
    public String formatter() {
        return TemplateFormatterDefine.SIZE;
    }

    @Override
    public Object format(Object value) {
        if(value instanceof Number) {
            int index = 0;
            for (int i=0; i < units.length; i++) {
                if (units[i].equals(this.unit)) {
                    index = i;
                    break;
                }
            }
            String unit = units[index];
            double time = ((Number) value).doubleValue();
            String nag = "";
            if(time < (double)0) {
                nag = "-";
                time = -time;
            }
            for(int i=index; (i+1) < powers.length && time>=powers[i+1]; i++) {
                time = time/powers[i+1];
                unit = units[i+1];
            }
            return nag + NumberUtils.formatDouble(time) + unit;
        }
        return value;
    }



}
