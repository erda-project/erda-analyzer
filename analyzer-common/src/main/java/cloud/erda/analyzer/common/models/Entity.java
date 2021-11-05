package cloud.erda.analyzer.common.models;/**
 * Created by luo on 2021/11/1.
 */


import lombok.Data;

import java.util.Map;

/**
 * Erda entity (https://yuque.antfin-inc.com/dice/zs3zid/kg8gis#nvqzT)
 *
 * @author Luo
 * @create 2021-11-01 2:08 PM
 **/
@Data
public class Entity {
    private String id;
    private String type;
    private String key;
    private Map<String, String> values;
    private Map<String, String> labels;
    private long createTimeUnixNano;
    private long updateTimeUnixNano;
}
