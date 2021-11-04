package cloud.erda.analyzer.errorInsight.functions;/**
 * Created by luo on 2021/11/1.
 */


import cloud.erda.analyzer.common.models.Entity;
import cloud.erda.analyzer.errorInsight.model.ErrorDescription;
import lombok.var;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Map;

/**
 * error to entity
 *
 * @author Luo
 * @create 2021-11-01 2:05 PM
 **/
public class ErrorDesc2ErdaEntityMapper implements MapFunction<ErrorDescription, Entity>{


    @Override
    public Entity map(ErrorDescription value) throws Exception {
        var entity = new Entity();

        entity.setCreateTimeUnixNano(value.getTimestamp());
        entity.setUpdateTimeUnixNano(value.getTimestamp());
        entity.setType("error_exception");
        entity.setKey(value.getErrorId());
        entity.setValues(value.getTags());
        Map<String, String> attributes = new HashedMap();
        attributes.put("terminusKey", value.getTerminusKey());
        attributes.put("applicationId", value.getApplicationId());
        attributes.put("serviceName", value.getServiceName());
        entity.setLabels(attributes);

        return entity;
    }
}
