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

import java.util.Date;

/**
 * @author: liuhaoyang
 * @create: 2020-01-08 11:50
 **/
public class TemplateFormatterFactory {

    public static TemplateFormatter create(String formatter) {
        if (formatter == null) {
            return null;
        }
        String[] split = formatter.split("\\:");
        if (split.length == 0) {
            return null;
        }
        String formatterDefine = split[0];
        String formatterUnit = "";
        if (split.length > 1) {
            formatterUnit = split[1];
        }
        TemplateFormatter templateFormatter = null;
        switch (formatterDefine) {
            case TemplateFormatterDefine.FRACTION:
                templateFormatter = new FractionTemplateFormatter(formatterUnit);
                break;
            case TemplateFormatterDefine.DATE:
                templateFormatter = new DateTemplateFormatter(formatterUnit);
                break;
            case TemplateFormatterDefine.TIME:
                templateFormatter = new TimeTemplateFormatter(formatterUnit);
                break;
            case TemplateFormatterDefine.PERCENT:
                templateFormatter = new PercentTemplateFormatter(formatterUnit);
                break;
            case  TemplateFormatterDefine.SIZE:
                templateFormatter = new SizeTemplateFormatter(formatterUnit);
                break;
            case TemplateFormatterDefine.STRING:
                templateFormatter = new StringTemplateFormatter(formatterUnit);
                break;
            default:
                break;
        }
        return templateFormatter;
    }

    // for test .
    public static void main(String[] args) throws Exception {
        TemplateFormatter format = new DateTemplateFormatter(null);
        System.out.println(format.format(System.currentTimeMillis())); // 2020-01-08 18:01:19
        System.out.println(format.format(new Date().getTime())); // 2020-01-08 18:01:19
        System.out.println(format.format(new Date())); // 2020-01-08 18:01:19

        format = new SizeTemplateFormatter("B");
        System.out.println(format.format( 8*1024*1024*1024L)); // 8GB
        System.out.println(format.format(-8*1024*1024L)); // -8MB
        format = new SizeTemplateFormatter("byte");
        System.out.println(format.format( 512*1024*1024L + 512*1024L)); // 512.5MB


        format = new SizeTemplateFormatter("DB");
        System.out.println(format.format(1025)); // 1,025DB
        System.out.println(format.format(-1024)); // -1,024DB

        format = new TimeTemplateFormatter("ns");
        System.out.println(format.format(1000*1000*1000L)); // 1s
        format = new TimeTemplateFormatter("d");
        System.out.println(format.format(30)); // 30d

        format = new PercentTemplateFormatter(null);
        System.out.println(format.format(1.00)); // 1%
        System.out.println(format.format(1.008)); // 1%
        System.out.println(format.format(9.9999)); // 10%
        System.out.println(format.format(-99.999)); // -100%

        format = new FractionTemplateFormatter("4");
        System.out.println(format.format(601.0d)); // 1
        System.out.println(format.format(1.008)); // 1
        System.out.println(format.format(9.9999)); // 10
        System.out.println(format.format(-99.999)); // -100
    }
}
