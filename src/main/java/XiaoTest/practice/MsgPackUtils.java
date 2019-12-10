package XiaoTest.practice;

import java.io.IOException;
import java.io.StringBufferInputStream;

import org.apache.commons.lang.StringEscapeUtils;
//import org.apache.commons.lang.StringEscapeUtils;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@SuppressWarnings("deprecation")
public class MsgPackUtils {
	private static final MessagePackFactory msgPackFac = new MessagePackFactory();
	
	public static JSONObject getDeserializedJSONObject(String data) throws JsonParseException, JsonMappingException, IOException {
		return new ObjectMapper(msgPackFac).readValue(new StringBufferInputStream(StringEscapeUtils.unescapeJava(data.replace("\\x", "\\u00"))), new TypeReference<JSONObject>(){});
		//return null;
	}
}
