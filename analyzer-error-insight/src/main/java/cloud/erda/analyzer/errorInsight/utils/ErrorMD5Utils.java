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

package cloud.erda.analyzer.errorInsight.utils;

import cloud.erda.analyzer.errorInsight.functions.ErrorConstants;
import cloud.erda.analyzer.errorInsight.model.ErrorEvent;
import org.apache.commons.codec.digest.DigestUtils;

/**
 * @author: liuhaoyang
 * @create: 2018-12-24 18:06
 **/
public class ErrorMD5Utils {
    public static String getMD5(ErrorEvent errorEvent) {
        String key = String.format("%s-%s-%s-%s-%s-%s",
                errorEvent.getTags().get(ErrorConstants.PROJECT_ID),
                errorEvent.getTags().get(ErrorConstants.WORKSPACE),
                errorEvent.getTags().get(ErrorConstants.RUNTIME_ID),
                errorEvent.getTags().get(ErrorConstants.SERVICE_NAME),
                errorEvent.getMetaData().get(ErrorConstants.ERROR_FILE),
                errorEvent.getMetaData().get(ErrorConstants.ERROR_LINE));
        return DigestUtils.md5Hex(key);
    }
}
