package utils;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;

public class StateDescriptors {
    public static final MapStateDescriptor<Long,String> diceOrgDescriptor =
            new MapStateDescriptor<>("dice_org", BasicTypeInfo.LONG_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO);
}
