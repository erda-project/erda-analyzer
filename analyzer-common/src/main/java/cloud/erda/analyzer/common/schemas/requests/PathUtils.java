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

public class PathUtils {

    public static String getRoute(String url) {
        return removeNumber(getPath(url));
    }

    // "/" -> /
    // "/abc" -> /abc
    // "/abc/123.0" -> /abc/{number}
    // "/abc/cef/" -> /abc/cef/
    // "/123" -> /{number}
    // "/123/456" -> /{number}/{number}
    // "/123/123/" -> /{number}/{number}/
    private static String removeNumber(String path) {
        String[] parts = path.split("\\/",-1);
        StringBuilder sb = new StringBuilder();
        for(int i=0, last = parts.length -1; i < parts.length; i++) {
            try {
                Double.parseDouble(parts[i]);
                sb.append("{number}");
            } catch (NumberFormatException e) {
                sb.append(parts[i]);
            }
            if(i != last) {
                sb.append("/");
            }
        }
        return sb.toString();
    }

    // "http://host/path1/path2?query1=value1&query2=value2" -> /path1/path2
    // "//host/path1/path2?query1=value1&query2=value2" -> /path1/path2
    // "/path1/path2?query1=value1&query2=value2" -> /path1/path2
    // "//?query1=value1&query2=value2" -> /
    // "//host?query1=value1&query2=value2" -> /
    // "path1/path2?query1=value1&query2=value2" -> /path1/path2
    public static String getPath(String url) {
        if (url == null || "".equals(url)) return "";
        int start = url.indexOf("//");
        if (start < 0) {
            start = 0;
        } else {
            url = url.substring(start + 2);
            start = url.indexOf("/");
            if (start < 0) {
                return "/";
            }
        }
        int end = url.indexOf("?");
        if (end < 0) {
            end = url.length();
        }
        url = url.substring(start, end);
        if (url.length() <= 0) {
            return "/";
        } else if (!url.startsWith("/")) {
            return "/" + url;
        }
        return url;
    }
}
