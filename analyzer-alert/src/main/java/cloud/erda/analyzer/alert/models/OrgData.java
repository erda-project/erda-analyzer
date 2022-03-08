package cloud.erda.analyzer.alert.models;

import cloud.erda.analyzer.runtime.models.ErrorMessage;
import lombok.Data;

import java.util.Map;

@Data
public class OrgData {
    private boolean success;
    private ErrorMessage err;
    private Map<String, String> data;
}
