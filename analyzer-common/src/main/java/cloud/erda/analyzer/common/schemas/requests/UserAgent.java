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

import com.decibel.uasparser.OnlineUpdater;
import com.decibel.uasparser.UASparser;
import com.decibel.uasparser.UserAgentInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class UserAgent {
    private final static Logger logger = LoggerFactory.getLogger(UserAgent.class);

    private static UASparser parser = null;
    static  {
        try {
            parser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (Exception e) {
            logger.error("init UASparser fail.", e);
        }
    }

    public final static String OTHER = "其他";

    public String browser = OTHER;
    public String browserVersion = "";
    public String os = OTHER;
    public String osVersion = "";
    public String device = OTHER;

    public static UserAgent parse(String ua) {
        UserAgent result = new UserAgent();
        if(ua == null || "".equals(ua)) {
            return result;
        }
        UserAgentInfo info = null;
        try {
            info = parser.parse(ua);
        } catch (IOException e) {
            return  result;
        }
        if(info == null) return result;
        String val = info.getOsName();
        if(val != null && !"unknown".equals(val)) {
            result.os = val;
        }
        val = info.getUaFamily();
        if(val != null && !"unknown".equals(val)) {
            result.browser = val;
        }
        val = info.getBrowserVersionInfo();
        if(val != null && !"unknown".equals(val)) {
            result.browserVersion = val;
        }
        val = info.getDeviceType();
        if(val != null && !"unknown".equals(val)) {
            result.device = val;
        }
        return result;
    }
}
