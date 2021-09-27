/*
 * Copyright (c) 2021 Terminus, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cloud.erda.analyzer.tracing.functions;

import cloud.erda.analyzer.common.utils.ConvertUtils;
import lombok.Data;

import java.io.Serializable;

/**
 * @author liuhaoyang
 * @date 2021/9/27 09:30
 */
@Data
public class FieldAggregator implements Serializable {

    private String name;

    private Long count;

    private Double sum;

    private Double min;

    private Double max;

    public FieldAggregator(String fieldName) {
        name = fieldName;
    }

    public void apply(Object value) {
        Double d = ConvertUtils.toDouble(value);
        if (d == null) {
            return;
        }
        if (count == null) {
            count = 0L;
            sum = 0D;
            max = min = d;
        }
        count++;
        sum += d;
        if (d < min) {
            min = d;
        }
        if (d > max) {
            max = d;
        }
    }

    public double getMean() {
        if (sum == null || count == null) {
            return 0;
        }
        return sum / count;
    }
}