package cloud.erda.analyzer.runtime.JsonDeserializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ArrayToStringDeserializer extends JsonDeserializer<String> {
    @Override
    public String deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
        if (jsonParser.currentToken() == JsonToken.START_ARRAY) {
            List<String> attributes = new ArrayList<>();
            jsonParser.nextToken();
            while (jsonParser.hasCurrentToken() && jsonParser.currentToken() != JsonToken.END_ARRAY) {
                attributes.add(jsonParser.getValueAsString());
                jsonParser.nextToken();
            }
            return String.join(",", attributes);
        } else {
            return jsonParser.getValueAsString();
        }
    }
}
