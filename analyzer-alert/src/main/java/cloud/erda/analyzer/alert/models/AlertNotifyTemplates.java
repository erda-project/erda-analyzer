package cloud.erda.analyzer.alert.models;

import lombok.Data;

import java.util.ArrayList;

@Data
public class AlertNotifyTemplates {
    private ArrayList<AlertNotifyTemplate> list;
    private int total;
}
