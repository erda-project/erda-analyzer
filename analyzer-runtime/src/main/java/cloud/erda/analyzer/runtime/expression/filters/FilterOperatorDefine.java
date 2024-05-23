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

package cloud.erda.analyzer.runtime.expression.filters;

/**
 * @author: liuhaoyang
 * @create: 2019-07-12 10:39
 **/
public class FilterOperatorDefine {

    public static final String Equal = "eq";

    public static final String InsensitivelyEqual = "ieq";

    public static final String NotEqual = "neq";

    public static final String Like = "like";

    public static final String Any = "any";

    public static final String Null = "null";

    public static final String In = "in";

    public static final String NotIn = "notIn";

    public static final String Script = "script";

    public static final String False = "false";

    public static final String Match = "match";

    public static final String NotMatch = "notMatch";

    public static final String All = "all";

    public static final String Default = False;
}