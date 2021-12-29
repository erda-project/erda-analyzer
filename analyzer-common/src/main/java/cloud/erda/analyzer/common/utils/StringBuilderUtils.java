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
 * @author liuhaoyang
 * @date 2021/12/28 16:20
 */
public class StringBuilderUtils {

    private static final int DEFAULT_STRING_BUILDER_SIZE = 1024;
    private static final ThreadLocal<StringBuilder> CACHED_STRINGBUILDERS =
            ThreadLocal.withInitial(() -> new StringBuilder(DEFAULT_STRING_BUILDER_SIZE));

    public static StringBuilder getCachedStringBuilder() {
        StringBuilder sb = CACHED_STRINGBUILDERS.get();
        sb.setLength(0);
        return sb;
    }
}
