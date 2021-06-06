package cloud.erda.analyzer.errorInsight.utils;

import cloud.erda.analyzer.common.utils.StringUtil;

public class MetricServiceId {
    public static String spliceServiceId(String applicationId,String runtimeName,String serviceName) {
        if (StringUtil.isEmpty(applicationId)) {
            if (StringUtil.isEmpty(runtimeName)) {
                return serviceName;
            }
            return runtimeName + "_" + serviceName;
        }
        return applicationId + "_" + runtimeName + "_" + serviceName;
    }
}
