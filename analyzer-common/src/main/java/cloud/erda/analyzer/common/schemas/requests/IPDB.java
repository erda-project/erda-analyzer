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
import qiniu.ip17mon.LocationInfo;
import qiniu.ip17mon.Locator;
import java.io.File;
import java.io.InputStream;
import java.net.URL;

public class IPDB implements java.io.Serializable {
    private final static Logger logger = LoggerFactory.getLogger(IPDB.class);
    private static Locator locator = null;

    public static LocationInfo find(String ip) {
        if(locator == null) {
            return new LocationInfo("中国","局域网","局域网","");
        }
        return locator.find(ip);
    }

    static {
        try {
            File file = getFile("ipdb.dat");
            if(file == null) {
                InputStream is = getStream("ipdb.dat");
                if(is != null) {
                    locator = Locator.loadFromStreamOld(is);
                    is.close();
                } else {
                    logger.error("ipdb.dat not exist");
                }
            } else {
                locator = Locator.loadFromLocal(file.getPath());
            }
        } catch (Exception e) {
            logger.error("ipdb load failed.",e);
        }
    }

    private static File getFile(String name) {
        File file = new File(name);
        if(!file.exists()) {
            URL url = IPDB.class.getClassLoader().getResource(name);
            if(url != null) {
                file = new File(url.getFile());
                if(file.exists()) {
                    return file;
                }
            }
            file = new File("/mnt/mesos/sandbox/" + name);
            if(file.exists()) {
                return file;
            }
            return null;
        }
        return file;
    }

    private static InputStream getStream(String name) {
        return IPDB.class.getClassLoader().getResourceAsStream(name);
    }
}
