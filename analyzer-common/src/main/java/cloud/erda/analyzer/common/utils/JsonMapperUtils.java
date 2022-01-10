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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.MapType;

import java.io.IOException;
import java.util.*;

/**
 * @author liuhaoyang
 * @date 2021/12/29 12:01
 */
public class JsonMapperUtils {

    private static final ThreadLocal<ObjectMapper> CACHED_OBJECTMAPPER = ThreadLocal.withInitial(() -> {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return objectMapper;
    });

    private static ObjectMapper getCachedObjectMapper() {
        return CACHED_OBJECTMAPPER.get();
    }

    private static final MapType OBJECT_VALUE_MAP_TYPE = constructMapType(HashMap.class, String.class, Object.class);
    private static final MapType STRING_VALUE_MAP_TYPE = constructMapType(HashMap.class, String.class, String.class);

    public static <T> T toObject(byte[] data, Class<T> type) throws IOException {
        return getCachedObjectMapper().readValue(data, type);
    }

    public static <T> T toObject(String data, Class<T> type) throws IOException {
        return getCachedObjectMapper().readValue(data, type);
    }

    public static Map<String, Object> toObjectValueMap(byte[] data) throws IOException {
        return getCachedObjectMapper().readValue(data, OBJECT_VALUE_MAP_TYPE);
    }

    public static Map<String, Object> toObjectValueMap(String data) throws IOException {
        return getCachedObjectMapper().readValue(data, OBJECT_VALUE_MAP_TYPE);
    }

    public static Map<String, String> toStringValueMap(byte[] data) throws IOException {
        return getCachedObjectMapper().readValue(data, STRING_VALUE_MAP_TYPE);
    }

    public static Map<String, String> toStringValueMap(String data) throws IOException {
        return getCachedObjectMapper().readValue(data, STRING_VALUE_MAP_TYPE);
    }


    public static <K, V> Map<K, V> toHashMap(byte[] data, Class<K> keyClazz, Class<V> valueClazz) throws IOException {
        return getCachedObjectMapper().readValue(data, constructMapType(HashMap.class, keyClazz, valueClazz));
    }

    public static <K, V> Map<K, V> toHashMap(String data, Class<K> keyClazz, Class<V> valueClazz) throws IOException {
        return getCachedObjectMapper().readValue(data, constructMapType(HashMap.class, keyClazz, valueClazz));
    }

    public static <T> byte[] toBytes(T value) throws IOException {
        return getCachedObjectMapper().writeValueAsBytes(value);
    }

    public static <T> String toStrings(T value) throws IOException {
        return getCachedObjectMapper().writeValueAsString(value);
    }

    public static <L extends Collection<T>, T> L toList(byte[] data, Class<L> listClass, Class<T> valueClazz) throws IOException {
        return getCachedObjectMapper().readValue(data, constructCollectionType(listClass, valueClazz));
    }

    public static <L extends Collection<T>, T> L toList(String data, Class<L> listClass, Class<T> valueClazz) throws IOException {
        return getCachedObjectMapper().readValue(data, constructCollectionType(listClass, valueClazz));
    }

    public static <T> ArrayList<T> toArrayList(byte[] data, Class<T> valueClazz) throws IOException {
        return getCachedObjectMapper().readValue(data, constructCollectionType(ArrayList.class, valueClazz));
    }

    public static <T> ArrayList<T> toArrayList(String data, Class<T> valueClazz) throws IOException {
        return getCachedObjectMapper().readValue(data, constructCollectionType(ArrayList.class, valueClazz));
    }

    private static MapType constructMapType(Class<? extends Map> mapClass, Class<?> keyClass, Class<?> valueClass) {
        return getCachedObjectMapper().getTypeFactory().constructMapType(mapClass, keyClass, valueClass);
    }

    public static CollectionType constructCollectionType(Class<? extends Collection> parametrized, Class<?> parameterClasses) {
        return getCachedObjectMapper().getTypeFactory().constructCollectionType(parametrized, parameterClasses);
    }
}
