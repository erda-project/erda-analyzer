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

package cloud.erda.analyzer.common.utils;

import javax.validation.constraints.NotNull;

public class StringUtil {
    /**
     * 判空
     *
     * @param str
     * @return
     */
    public static boolean isEmpty(String str) {
        return str == null || str.trim().length() == 0;
    }

    /**
     * 判非空
     *
     * @param str
     * @return
     */
    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }


    /**
     * 包含
     *
     * @param str1
     * @param str2
     * @return
     */
    public static boolean isContains(@NotNull String str1, @NotNull String str2) {
        return str1.contains(str2);
    }

    /**
     * 不包含
     *
     * @param str1
     * @param str2
     * @return
     */
    public static boolean isNotContains(@NotNull String str1, @NotNull String str2) {
        return !isContains(str1, str2);
    }
}