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

public class NumberParser {

    public static double parseDouble(String val, double defVal) {
        try {
            return Double.parseDouble(val);
        } catch (Exception e) {
            return defVal;
        }
    }

    public static long parseLong(String val, long defVal) {
        try {
            return Long.parseLong(val);
        } catch (Exception e) {
            return defVal;
        }
    }

    public static long parseLong(String val, long defVal, int radix) {
        try {
            return Long.parseLong(val, radix);
        } catch (Exception e) {
            return defVal;
        }
    }

    public static int parseInt(String val, int defVal) {
        try {
            return Integer.parseInt(val);
        } catch (Exception e) {
            return defVal;
        }
    }

}
