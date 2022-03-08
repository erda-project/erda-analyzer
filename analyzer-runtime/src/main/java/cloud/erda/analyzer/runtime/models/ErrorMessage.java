package cloud.erda.analyzer.runtime.models;

import lombok.Data;

@Data
public class ErrorMessage {
    private String code;
    private String msg;
    private String ctx;
}