package cloud.erda.analyzer.alert.models;

import lombok.Data;

import java.util.ArrayList;

@Data
public class AlertNotifyData {
    private ArrayList<AlertNotify> list;
    private int total;
}
