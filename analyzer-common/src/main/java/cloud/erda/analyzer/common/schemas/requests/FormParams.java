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

package cloud.erda.analyzer.common.schemas.requests;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;

public class FormParams extends HashMap<String, String> {

    private final static Logger logger = LoggerFactory.getLogger(FormParams.class);

    // parse("key1=val1&key2=val 2&key3=val%203") = {"key1":"val1","key2":"val 2","key3":"val 3"}
    public static FormParams parse(String queryString) {
        FormParams params = new FormParams();
        String[] kvs = queryString.split("\\&");
        try {
            int idx = -1;
            for(String item : kvs) {
                idx = item.indexOf("=");
                if(idx > 0 && idx < item.length() - 1) {
                    String key = item.substring(0, idx); // URLDecoder.decode(item.substring(0, idx);,"UTF-8");
                    params.put(key, URLDecoder.decode(item.substring(idx + 1),"UTF-8"));
                }
            }
        } catch (UnsupportedEncodingException e) {
            logger.error("invalid data in message" + queryString);
            return null;
        }
        return params;
    }
}
