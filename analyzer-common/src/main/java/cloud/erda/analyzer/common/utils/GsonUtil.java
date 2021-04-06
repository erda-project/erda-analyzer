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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Map;

public class GsonUtil {
    private final static Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();

    public static <T> T toObject(String value, Class<T> type) {
        return gson.fromJson(value, type);
    }

    public static <T> ArrayList<T> toArrayList(String value, Class<T> type) {
        return gson.fromJson(value,TypeToken.getParameterized(ArrayList.class,type).getType());

    }

    public static <K, V> Map<K, V> toMap(String json, Class<K> keyType, Class<V> valueType) {
        return gson.fromJson(json, TypeToken.getParameterized(Map.class, keyType, valueType).getType());
    }

    public static String toJson(Object value) {
        return gson.toJson(value);
    }

    public static byte[] toJSONBytes(Object value) {
        return gson.toJson(value).getBytes(Charset.forName("UTF-8"));
    }
}
