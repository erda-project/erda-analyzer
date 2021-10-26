package cloud.erda.analyzer.common.models;

import lombok.Data;
import java.util.HashMap;

@Data
public class Event {
    private String eventID;

    private String severity;

    private String name;

    private EventKind kind;

    private long timeUnixNano;

    private Relation relations;

    private HashMap<String, String> attributes;

    private String message;
}

