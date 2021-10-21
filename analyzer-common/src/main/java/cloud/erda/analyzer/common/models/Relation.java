package cloud.erda.analyzer.common.models;

import lombok.Data;

@Data
public class Relation {
    private String traceID;

    private String resID;

    private String resType;

    private String[] resourceKeys;
}
