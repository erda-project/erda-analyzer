package cloud.erda.analyzer.errorInsight.functions;/**
 * Created by luo on 2021/11/1.
 */


import cloud.erda.analyzer.common.constant.ExceptionConstants;
import cloud.erda.analyzer.common.models.*;
import cloud.erda.analyzer.errorInsight.model.ErrorEvent;
import lombok.var;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashMap;

/**
 * error_event to erda_event
 *
 * @author Luo
 * @create 2021-11-01 11:09 AM
 **/
public class ErrorEvent2ErdaEventMapper implements MapFunction<ErrorEvent, Event> {
    @Override
    public Event map(ErrorEvent value) throws Exception {
        var erdaEvent = new Event();

        erdaEvent.setEventID(value.getEventId());
        erdaEvent.setKind(EventKind.EVENT_KIND_EXCEPTION);
        erdaEvent.setTimeUnixNano(value.getTimestamp());
        erdaEvent.setName(EventNameConstants.EXCEPTION);
        erdaEvent.setMessage(JSONArray.toJSONString(value.getStacks()));

        Relation relation = new Relation();
        relation.setResID(value.getErrorId());
        relation.setResType(RelationTypeConstants.EXCEPTION);
        erdaEvent.setRelations(relation);

        HashMap<String, String> attributes = new HashMap<>();
        attributes.put(ExceptionConstants.REQUEST_ID, value.getRequestId());
        attributes.put(ExceptionConstants.TERMINUS_KEY, value.getTags().getOrDefault("terminus_key", "defaultKey"));
        attributes.put(ExceptionConstants.META_DATA, JSONObject.toJSONString(value.getMetaData()));
        attributes.put(ExceptionConstants.TAGS, JSONObject.toJSONString(value.getTags()));
        attributes.put(ExceptionConstants.REQUEST_CONTEXT, JSONObject.toJSONString(value.getRequestContext()));
        attributes.put(ExceptionConstants.REQUEST_HEADERS, JSONObject.toJSONString(value.getRequestHeaders()));
        erdaEvent.setAttributes(attributes);

        return erdaEvent;
    }
}
