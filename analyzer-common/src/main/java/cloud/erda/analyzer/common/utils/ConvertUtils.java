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

package cloud.erda.analyzer.common.utils;

/**
 * @author: liuhaoyang
 * @create: 2019-06-30 16:19
 **/
public class ConvertUtils {

    public static Double toDouble(Object value) {
        if (value == null) {
            return null;
        }
        Class<?> valType = value.getClass();
        if (valType.equals(Integer.class)) {
            return Double.valueOf(value.toString());
        }
        if (valType.equals(Long.class)) {
            return Double.valueOf(value.toString());
        }
        if (valType.equals(Float.class)) {
            return Double.valueOf(value.toString());
        }
        if (valType.equals(Double.class)) {
            return Double.valueOf(value.toString());
        }
        if (valType.equals(Boolean.class)) {
            return Boolean.valueOf(value.toString()) ? 1D : 0D;
        }

        return null;
    }

    public static <E extends Enum> E toEnum(Object value, Class<E> type) {
        if (value == null) {
            return null;
        }
        return (E) Enum.valueOf(type, value.toString().toUpperCase());
    }
}
