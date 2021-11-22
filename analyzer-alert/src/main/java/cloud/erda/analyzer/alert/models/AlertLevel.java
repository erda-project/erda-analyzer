package cloud.erda.analyzer.alert.models;

import cloud.erda.analyzer.common.constant.AlertConstants;
import cloud.erda.analyzer.common.models.MetricEvent;
import cloud.erda.analyzer.common.utils.StringUtil;

import java.util.HashMap;
import java.util.Map;

public enum AlertLevel {
    UNKNOWN,
    FATAL,
    CRITICAL,
    WARNING,
    NOTICE;

    private static final Map<String, AlertLevel> levelConverts = new HashMap<>();

    static {
        // compatible with the level definition of the previous version
        levelConverts.put("BREAKDOWN", AlertLevel.FATAL);
        levelConverts.put("EMERGENCY", AlertLevel.CRITICAL);
        levelConverts.put("ALERT", AlertLevel.WARNING);
        levelConverts.put("LIGHT", AlertLevel.NOTICE);

        levelConverts.put("ERROR", AlertLevel.CRITICAL);
        levelConverts.put("INFO", AlertLevel.NOTICE);

        levelConverts.put(AlertLevel.FATAL.name(), AlertLevel.FATAL);
        levelConverts.put(AlertLevel.CRITICAL.name(), AlertLevel.CRITICAL);
        levelConverts.put(AlertLevel.WARNING.name(), AlertLevel.WARNING);
        levelConverts.put(AlertLevel.NOTICE.name(), AlertLevel.NOTICE);
    }

    public static AlertLevel of(AlertEvent alertEvent) {
        return of(alertEvent.getMetricEvent());
    }

    public static AlertLevel of(MetricEvent metricEvent) {
        String levelTag = metricEvent.getTags().get(AlertConstants.ALERT_LEVEL);
        return of(levelTag);
    }

    public static AlertLevel of(String level) {
        if (StringUtil.isEmpty(level)) {
            return AlertLevel.UNKNOWN;
        }
        return levelConverts.getOrDefault(level.toUpperCase(), AlertLevel.UNKNOWN);
    }
}