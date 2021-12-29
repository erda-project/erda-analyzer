package cloud.erda.analyzer.alert.models;

import lombok.Data;

import java.util.ArrayList;

@Data
public class AlertNotifyTemplateData {
    private ArrayList<AlertNotifyTemplate> list;
    private int total;
}
