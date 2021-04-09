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

package cloud.erda.analyzer.runtime.utils;

import cloud.erda.analyzer.common.constant.MetricConstants;
import cloud.erda.analyzer.runtime.models.ExpressionMetadata;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;

import java.util.Map;

/**
 * @author: liuhaoyang
 * @create: 2019-06-29 10:32
 **/
public class StateDescriptorFactory {

    public static MapStateDescriptor<String, String> stringMapState(String name) {
        return new MapStateDescriptor<>(name, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
    }

    public static MapStateDescriptor<String, Long> longMapState(String name) {
        return new MapStateDescriptor<>(name, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);
    }

    public static <K, V> MapStateDescriptor<String, Map<K, V>> mapValueMapState(String name, TypeInformation<K> keyType, TypeInformation<V> valueType) {
        return new MapStateDescriptor<>(name, BasicTypeInfo.STRING_TYPE_INFO, new MapTypeInfo<>(keyType, valueType));
    }

    public static <T> MapStateDescriptor<String, T> intMapState(String name, Class<T> type) {
        return new MapStateDescriptor<>(name, BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(type));
    }

    public final static MapStateDescriptor<String, String> AlertKeyFilterState = stringMapState(MetricConstants.ALERT_KEY_FILTER_STATE);

    public final static MapStateDescriptor<String, String> MetricFilterState = stringMapState(MetricConstants.METRIC_NAME_FILTER_STATE);

    public final static MapStateDescriptor<String, Long> OfflineMachineFilterState = longMapState(MetricConstants.OFFLINE_MACHINE_FILTER_STATE);

    public final static MapStateDescriptor<String, Map<String, Long>> MetricKeyedMapState = mapValueMapState(MetricConstants.METRIC_KEYED_MAP_STATE, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);

    public final static MapStateDescriptor<String, ExpressionMetadata> ExpressionState = intMapState(MetricConstants.ALERT_EXPRESSION_STATE, ExpressionMetadata.class);
}
