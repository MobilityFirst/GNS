
package edu.umass.cs.gnscommon.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.FloatValue;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;


public class JSONByteConverter {
	//private static ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory()).setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
	private static ObjectMapper objectMapperJackson = new ObjectMapper().setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
	
	//private static TypeReference<Map<String,Object>> typeRef = new TypeReference<Map<String, Object>>(){};
	//private static TypeReference<JSONObject> typeRef = new TypeReference<JSONObject>(){};
	private static Class<? extends JSONObject> jsonClass = new JSONObject().getClass();
	

	private static final byte STRING_INDICATOR = 0;
	private static final byte ARRAY_INDICATOR = 1;
	private static final byte MAP_INDICATOR = 2;
	private static final byte INTEGER_INDICATOR = 3;
	private static final byte LONG_INDICATOR = 4;
	private static final byte FLOAT_INDICATOR = 5;
	private static final byte DOUBLE_INDICATOR = 6;
	private static final byte BOOLEAN_INDICATOR = 7;
	
	//private static ByteBuffer fourByteBuffer = ByteBuffer.allocate(4);
	//private static ByteBuffer eightByteBuffer = ByteBuffer.allocate(8);
	
	

	public static byte[] toBytesJackson(JSONObject json) throws JsonProcessingException{
		byte[] bytes = objectMapperJackson.writeValueAsBytes(json);
		return bytes;
	}
	
	
	
	

	public static JSONObject fromBytesJackson(byte[] bytes) throws JsonParseException, JsonMappingException, IOException, JSONException{
		//ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
		//@SuppressWarnings("rawtypes") //We cast to Map so JSONObject can use the correct constructor.
		//JSONObject json = new JSONObject((Map)objectMapper.readValue(bytes, typeRef));
		
		//JSONObject json = objectMapper.readValue(bytes, typeRef);
		

		JSONObject json = objectMapperJackson.readValue(bytes, jsonClass);
		return json;
	}


	public static JSONObject fromBytesJackson(ByteBuffer bbuf)
			throws JsonParseException, JsonMappingException, IOException,
			JSONException {
		return objectMapperJackson
				.readValue(
						Arrays.copyOfRange(bbuf.array(), bbuf.position(),
								bbuf.limit()), jsonClass);
	}


	private static void packJSONValue(Object value, MessageBufferPacker packer) throws JSONException, IOException{
		if (value instanceof JSONArray){
			packJSONArray((JSONArray)value, packer);
		}
		else if (value instanceof Integer){
			packer.packInt((int)value);
		}
		else if (value instanceof Long){
			packer.packLong((long)value);
		}
		else if (value instanceof Boolean){
			packer.packBoolean((boolean)value);
		}
		else if (value instanceof Float){
			packer.packFloat((float)value);
		}
		else if (value instanceof Double){
			packer.packDouble((double)value);
		}
		else if (value instanceof String){
			packer.packString((String)value);
		}
		else if (value instanceof JSONObject){
			packJSONObject((JSONObject)value, packer);
		}
	}
	

	private static void packJSONArray(JSONArray array, MessageBufferPacker packer) throws JSONException, IOException{
		int length = array.length();
		packer.packArrayHeader(length);
		for (int i = 0; i < length; i++){
			Object item = array.get(i);
			packJSONValue(item,packer);
		}
	}
	

	private static void packJSONObject(JSONObject json, MessageBufferPacker packer) throws JSONException, IOException{
		@SuppressWarnings("unchecked") // Assumption: All keys in the json are strings.
		Iterator<String> iterator = json.keys();
		int length = json.length();
		packer.packMapHeader(length);
		while (iterator.hasNext()){
			String key = iterator.next();
			Object value = json.get(key);
			packJSONValue(key, packer);
			packJSONValue(value,packer);
		}
	}
	

	@SuppressWarnings("unchecked") //JSON keys are assumed to be strings.
	public static byte[] toBytesMsgpack(JSONObject json) throws JSONException, IOException{
		MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
		packJSONObject(json, packer);
		return packer.toByteArray();
	}
	

	private static Object unpackValue(Value value, MessageUnpacker unpacker) throws JSONException{
		switch (value.getValueType()) {
        case BOOLEAN:
            boolean b = value.asBooleanValue().getBoolean();
            return b;
        case INTEGER:
            IntegerValue iv = value.asIntegerValue();
            if (iv.isInIntRange()) {
                int num = iv.toInt();
                return num;
            }
            else if (iv.isInLongRange()) {
                long l = iv.toLong();
                return l;
            }
            else {
                BigInteger big = iv.toBigInteger();
                return big;
            }
        case FLOAT:
            FloatValue fv = value.asFloatValue();
            //float f = fv.toFloat();
            double d = fv.toDouble();
            return d;
        case STRING:
            String s = value.asStringValue().asString();
            return s;
        case ARRAY:
            ArrayValue a = value.asArrayValue();
            JSONArray array = unpackArray(a, unpacker);
            return array;
        case MAP:
            MapValue m = value.asMapValue();
            JSONObject map = unpackMap(m, unpacker);
            return map;
		default:
			throw new JSONException("Unknown type to pack into json!");
		 }
	}

	private static JSONArray unpackArray(ArrayValue av, MessageUnpacker unpacker) throws JSONException{
		JSONArray array = new JSONArray();
		for (Value v : av){
			Object value = unpackValue(v, unpacker);
			array.put(value);
		}
		return array;
	}
	

	private static JSONObject unpackMap(MapValue mv, MessageUnpacker unpacker) throws JSONException{
		JSONObject json = new JSONObject();
		for (Entry<Value,Value> entry : mv.entrySet()){
			String key = entry.getKey().asStringValue().asString();
			Value value = entry.getValue();
			 Object javaValue = unpackValue(value, unpacker);
			 json.put(key, javaValue);
		}
		return json;
	}
	

	public static JSONObject fromBytesMsgpack(byte[] bytes) throws JsonParseException, JsonMappingException, IOException, JSONException{
		MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes);
		Value value = unpacker.unpackValue();
		JSONObject json = unpackMap(value.asMapValue(), unpacker);
		return json;
	}
	// FIXME: eliminate copyOfRange


	public static JSONObject fromBytesMsgpack(ByteBuffer bbuf) throws JsonParseException, JsonMappingException, IOException, JSONException{
		return fromBytesMsgpack(Arrays.copyOfRange(bbuf.array(), bbuf.position(), bbuf.limit())); 
	}

	private static final void byteJSONValue(Object value, ByteArrayOutputStream out) throws JSONException, IOException{
		ByteBuffer fourByteBuffer = ByteBuffer.allocate(4);
		ByteBuffer eightByteBuffer = ByteBuffer.allocate(8);
		if (value instanceof JSONArray){
			//byteJSONArray((JSONArray)value, out);
			JSONArray array = (JSONArray)value;
			int length = array.length();
			out.write(ARRAY_INDICATOR);
			fourByteBuffer.rewind();
			fourByteBuffer.putInt(length);
			out.write(fourByteBuffer.array());
			for (int i = 0; i < length; i++){
				byteJSONValue(array.get(i),out);
			}
		}
		else if (value instanceof Integer){
			out.write(INTEGER_INDICATOR);
			fourByteBuffer.rewind();
			fourByteBuffer.putInt((Integer)value);
			out.write(fourByteBuffer.array());
		}
		else if (value instanceof Long){
			out.write(LONG_INDICATOR);
			eightByteBuffer.rewind();
			eightByteBuffer.putLong((Long)value);
			out.write(eightByteBuffer.array());
			
		}
		else if (value instanceof Boolean){
			out.write(BOOLEAN_INDICATOR);
			if ((Boolean)value){
				out.write(1);
			}
			else{
				out.write(0);
			}
			
		}
		else if (value instanceof Float){
			out.write(FLOAT_INDICATOR);
			fourByteBuffer.rewind();
			fourByteBuffer.putFloat((Float)value);
			out.write(fourByteBuffer.array());
		}
		else if (value instanceof Double){
			out.write(DOUBLE_INDICATOR);
			eightByteBuffer.rewind();
			eightByteBuffer.putDouble((Double)value);
			out.write(eightByteBuffer.array());
		}
		else if (value instanceof String){
			byte[] stringBytes = ((String)value).getBytes();
			out.write(STRING_INDICATOR);
			int length = stringBytes.length;
			fourByteBuffer.rewind();
			fourByteBuffer.putInt(length);
			out.write(fourByteBuffer.array());
			out.write(stringBytes);
		}
		else if (value instanceof JSONObject){
			//byteJSONObject((JSONObject)value, out);
			JSONObject json = (JSONObject) value;
			@SuppressWarnings("unchecked") // Assumption: All keys in the json are strings.
			Iterator<String> iterator = json.keys();
			int length = json.length();
			out.write(MAP_INDICATOR);
			fourByteBuffer.rewind();
			fourByteBuffer.putInt(length);
			out.write(fourByteBuffer.array());
			while (iterator.hasNext()){
				String key = iterator.next();
				Object val = json.get(key);
				byteJSONValue(key,out);
				byteJSONValue(val,out);
			}
		}
		else{
			throw new JSONException("UNKNOWN TYPE IN JSON!");
		}
	}
	
	private static final void byteJSONValue(Object value, ByteBuffer out) throws JSONException, IOException{
		if (value instanceof JSONArray){
			//byteJSONArray((JSONArray)value, out);
			JSONArray array = (JSONArray)value;
			int length = array.length();
			out.put(ARRAY_INDICATOR);
			out.putInt(length);
			for (int i = 0; i < length; i++){
				byteJSONValue(array.get(i),out);
			}
		}
		else if (value instanceof Integer){
			out.put(INTEGER_INDICATOR);
			out.putInt((Integer)value);
		}
		else if (value instanceof Long){
			out.put(LONG_INDICATOR);
			out.putLong((Long)value);
			
		}
		else if (value instanceof Boolean){
			out.put(BOOLEAN_INDICATOR);
			if ((Boolean)value){
				out.put((byte)1);
			}
			else{
				out.put((byte)0);
			}
			
		}
		else if (value instanceof String){
			byte[] stringBytes = ((String)value).getBytes();
			out.put(STRING_INDICATOR);
			int length = stringBytes.length;
			out.putInt(length);
			out.put(stringBytes);
		}
		else if (value instanceof JSONObject){
			//byteJSONObject((JSONObject)value, out);
			JSONObject json = (JSONObject) value;
			@SuppressWarnings("unchecked") // Assumption: All keys in the json are strings.
			Iterator<String> iterator = json.keys();
			int length = json.length();
			out.put(MAP_INDICATOR);
			out.putInt(length);
			while (iterator.hasNext()){
				String key = iterator.next();
				Object val = json.get(key);
				byteJSONValue(key,out);
				byteJSONValue(val,out);
			}
		}
		else{
			throw new JSONException("UNKNOWN TYPE IN JSON!");
		}
	}


	public static final byte[] toBytesHardcoded(JSONObject json) throws JSONException, IOException{
		ByteArrayOutputStream out = new ByteArrayOutputStream(1200);
//		ByteBuffer bbuf = ByteBuffer.wrap(new byte[1200]);
//		byteJSONValue(json, bbuf);
//		byte[] bytes= bbuf.array();
		byteJSONValue(json, out);
		out.close();
		byte[] bytes = out.toByteArray();
		return bytes;
	}
	

	private static Object valueFromBytes(ByteBuffer bytes) throws JSONException{
		byte type = bytes.get();
		//System.out.println("From value: " + new Integer(type).toString());
		switch(type){
		case MAP_INDICATOR:
			//System.out.println("MAP!");
			return mapFromBytes(bytes);
		case ARRAY_INDICATOR:
			return arrayFromBytes(bytes);
		case STRING_INDICATOR:
			//System.out.println("STRING!");
			int length = bytes.getInt();
			byte[] stringBytes = new byte[length];
			bytes.get(stringBytes, 0, length);
			return new String(stringBytes);
		case INTEGER_INDICATOR:
			return bytes.getInt();
		case LONG_INDICATOR:
			return bytes.getLong();
		case DOUBLE_INDICATOR:
			return bytes.getDouble();
		case FLOAT_INDICATOR:
			return bytes.getFloat();
		case BOOLEAN_INDICATOR:
			return bytes.get() == 1 ? 1: 0;
		default:
			throw new JSONException("Tried to decode unknown type from byte array!");
		}
		
			
			
	}
	

	private static JSONObject mapFromBytes(ByteBuffer bytes) throws JSONException{
		JSONObject json = new JSONObject();
		//System.out.println("From map!");
		int length = bytes.getInt();
		//System.out.println("Length " + Integer.toString(length));
		for (int i = 0; i < length; i++){
			//String key =(String) valueFromBytes(bytes);
			
			//Get Key
			byte type = bytes.get();
			//assert(type == STRING_INDICATOR);
			int lengthKey = bytes.getInt();
			byte[] stringBytes = new byte[lengthKey];
			bytes.get(stringBytes, 0, lengthKey);
			String key = new String(stringBytes);
			//System.out.println("From map: " + new Integer(type).toString());
			//Get value
			type = bytes.get();
			switch(type){
			case MAP_INDICATOR:
				json.put(key, mapFromBytes(bytes));
				break;
			case ARRAY_INDICATOR:
				json.put(key, arrayFromBytes(bytes));
				break;
			case STRING_INDICATOR:
				int lengthString = bytes.getInt();
				stringBytes = new byte[lengthString];
				bytes.get(stringBytes, 0, lengthString);
				json.put(key,new String(stringBytes));
				break;
			case INTEGER_INDICATOR:
				json.put(key,bytes.getInt());
				break;
			case LONG_INDICATOR:
				json.put(key,bytes.getLong());
				break;
			case DOUBLE_INDICATOR:
				json.put(key,bytes.getDouble());
				break;
			case FLOAT_INDICATOR:
				json.put(key,bytes.getFloat());
				break;
			default:
				throw new JSONException("Tried to decode unknown type from byte array!");
			}
		}
		return json;
	}
	

	private static JSONArray arrayFromBytes(ByteBuffer bytes) throws JSONException{
		JSONArray json = new JSONArray();
		int length = bytes.getInt();
		for (int i = 0; i < length; i++){
			//Object value = valueFromBytes(bytes);
			//json.put(value);
			//Get value
			byte type = bytes.get();
			switch(type){
			case MAP_INDICATOR:
				json.put(mapFromBytes(bytes));
				break;
			case ARRAY_INDICATOR:
				json.put(arrayFromBytes(bytes));
				break;
			case STRING_INDICATOR:
				int lengthString = bytes.getInt();
				byte[] stringBytes = new byte[lengthString];
				bytes.get(stringBytes, 0, lengthString);
				json.put(new String(stringBytes));
				break;
			case INTEGER_INDICATOR:
				json.put(bytes.getInt());
				break;
			case LONG_INDICATOR:
				json.put(bytes.getLong());
				break;
			case DOUBLE_INDICATOR:
				json.put(bytes.getDouble());
				break;
			case FLOAT_INDICATOR:
				json.put(bytes.getFloat());
				break;
			default:
				throw new JSONException("Tried to decode unknown type from byte array!");
			}
		}
		return json;
	}
	

	public static JSONObject fromBytesHardcoded(byte[] bytes) throws JSONException{
		ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
		//byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
		//ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length);
		//byteBuffer.put(bytes);
		//byteBuffer.rewind();
		JSONObject obj = (JSONObject) valueFromBytes(byteBuffer);
		return obj;
	}


  public static JSONObject fromBytesHardcoded(ByteBuffer bbuf) throws JSONException{
		return (JSONObject)valueFromBytes(bbuf);
	}

}
