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

package cloud.erda.analyzer.runtime.utils;

import java.util.Arrays;

/**
 * Desc: 计算一个数组内的百分位数值，eg: p75、p95、
 */
public class PercentUtils {

    /**
     * @param data 数组（可以是乱序）
     * @param p 分位数(75、90、95等)
     * @return
     */
    public static double percent(double[] data, double p) {
        int n = data.length;
        Arrays.sort(data);

        double px = p * (n - 1) / 100;
        int i = (int) java.lang.Math.floor(px);
        double g = px - i;
        if (g == 0) {
            return data[i];
        } else {
            return (1 - g) * data[i] + g * data[i + 1];
        }
    }
}
