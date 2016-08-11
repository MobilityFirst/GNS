package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Message;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class ActiveMessage implements Message{
	
	private final static String CHARSET = "ISO-8859-1";
	private final static AtomicLong counter = new AtomicLong();
	
	/**
	 * Message type
	 */
	public Type type;
	private long id;
	private int ttl;	
	private String guid;
	private String field;
	private String code;
	private ValuesMap value;
	private String targetGuid;
	private String error;

	/**
	 * This enum represents the type of this ActiveMessage
	 * @author gaozy
	 *
	 */
	public static enum Type {
		/**
		 * This message is sent from GNS side to worker
		 * to run a piece of active code
		 */
		REQUEST(0),
		
		/**
		 * This message could be used for:
		 * <p>1. worker to send back execution result to GNS
		 * <p>2. GNS to send back query result to worker
		 */
		RESPONSE(1),
		
		/**
		 * This message is used for worker to send a read query
		 * to GNS to read a value of a field.
		 */
		READ_QUERY(2), 
		
		/**
		 * This message is used for worker to send a write query
		 * to GNS to update a field.
		 */
		WRITE_QUERY(3);		
		
		private final int type;
		Type(int type){
			this.type = type;
		}
		
		/**
		 * @return the type of this message
		 */
		public int getType() {
			return type;
		}

	}
	
	/**
	 * @param type 
	 * @param id 
	 * @param guid
	 * @param field
	 * @param code
	 * @param ttl
	 * @param value  
	 * @param targetGuid 
	 * @param error 
	 */
	public ActiveMessage(Type type, long id, int ttl, String guid, String field, String code, ValuesMap value, String targetGuid, String error){
		this.type = type;
		this.id = id;
		this.ttl = ttl;
		this.guid = guid;
		this.field = field;
		this.code = code;		
		this.value = value;
		this.targetGuid = targetGuid;
		this.error = error;
	}
	
	/**
	 * This is a REQUEST message
	 * @param guid
	 * @param field
	 * @param code
	 * @param value
	 * @param ttl
	 */
	public ActiveMessage(String guid, String field, String code, ValuesMap value, int ttl){
		this(Type.REQUEST, counter.getAndIncrement(), ttl, guid, field, code, value, null, null);
	}
	
	/**
	 * This is a READ_QUERY message
	 * @param ttl
	 * @param guid
	 * @param field
	 * @param targetGuid
	 */
	public ActiveMessage(int ttl, String guid, String field, String targetGuid){
		this(Type.READ_QUERY, counter.getAndIncrement(), ttl, guid, field, null, null, targetGuid, null);
	}
	
	
	/**
	 * This is a WRITE_QUERY message
	 * @param ttl
	 * @param guid
	 * @param field
	 * @param targetGuid
	 * @param value
	 */
	public ActiveMessage(int ttl, String guid, String field, String targetGuid, ValuesMap value){
		this(Type.WRITE_QUERY, counter.getAndIncrement(), ttl, guid, field, null, value, targetGuid, null);
	}
	
	/**
	 * This is a RESPONSE message
	 * @param id 
	 * @param value
	 * @param error
	 */
	public ActiveMessage(long id, ValuesMap value, String error){
		this(Type.RESPONSE, id, 0, null, null, null, value, null, error);
	}
	
	/**
	 * @return the TTL left for the request
	 */
	public int getTtl() {
		return ttl;
	}

	/**
	 * @return the GUID of the querier
	 */
	public String getGuid() {
		return guid;
	}

	/**
	 * @return the field to be operated on
	 */
	public String getField() {
		return field;
	}
	
	/**
	 * @return the GUID to be operated on
	 */
	public String getTargetGuid() {
		return targetGuid;
	}
	
	/**
	 * @return the code to be run
	 */
	public String getCode() {
		return code;
	}

	/**
	 * @return value
	 */
	public ValuesMap getValue() {
		return value;
	}
	
	/**
	 * @return error
	 */
	public String getError(){
		return error;
	}
	
	/**
	 * @return id of the message
	 */
	public long getId(){
		return id;
	}
		
	
	private int getEstimatedLengthExceptForValuesMap(){
		int length = 0;
		switch(type){
		case REQUEST:
			length = 6*Integer.BYTES // type, ttl, guid length, field length, code length, valuesMap size 
			+ Long.BYTES // id
			+ guid.length() // guid
			+ (field!=null?field.length():0) // field
			+ code.length();
			break;
			
		case RESPONSE:
			length = 3*Integer.BYTES // type, ttl, error length
			+ Long.BYTES // id
			+ (error != null?error.length():0);
			break;
			
		case READ_QUERY:
			length = 5*Integer.BYTES // type, ttl, guid length, field length, targetGuid length
			+ Long.BYTES // id
			+ guid.length()
			+ field.length()
			+ targetGuid.length();
			break;
			
		case WRITE_QUERY:
			length = 5*Integer.BYTES // type, ttl, guid length, field length, targetGuid length
			+ Long.BYTES // id
			+ guid.length()
			+ field.length()
			+ targetGuid.length();
			break;
			
		default:
			break;
		}

		return (int) (length*1.25);
	}
	
	
	/**
	 * @return the byte array being serialized
	 * @throws UnsupportedEncodingException
	 */
	@Override
	public byte[] toBytes() throws UnsupportedEncodingException{
		// First convert ValuesMap to String, as it is costly
		String valuesMapString = (value == null)?null:value.toString();
		
		byte[] buffer = new byte[this.getEstimatedLengthExceptForValuesMap()+( (valuesMapString==null)?0:valuesMapString.length() )];
		ByteBuffer bbuf = ByteBuffer.wrap(buffer);
		byte[] guidBytes,fieldBytes,codeBytes,valuesMapBytes,targetGuidBytes;
		
		// put type and request id
		bbuf.putInt(type.getType());
		int exactLength = Integer.BYTES;
		bbuf.putLong(id);
		exactLength += Long.BYTES;
		
		switch(type){
		case REQUEST:
			// put ttl 
			bbuf.putInt(ttl);
			exactLength += Integer.BYTES;
			
			// put guid, can't be null, ~100ns
			assert(guid != null):"guid can't be null for active request";
			guidBytes = guid.getBytes(CHARSET);
			bbuf.putInt(guidBytes.length);
			bbuf.put(guidBytes);
			exactLength += (Integer.BYTES + guidBytes.length);
			
			// put field, can't be null, ~100ns
			// assert(field != null):"field can't be null for active request";
			fieldBytes = (field!=null)?field.getBytes(CHARSET):new byte[0];
			bbuf.putInt((field!=null)?fieldBytes.length:0);
			bbuf.put(fieldBytes);
			exactLength += (Integer.BYTES + fieldBytes.length);
			
			// put code, can be null
			assert(code != null):"code can't be null for active request";
			codeBytes = code.getBytes(CHARSET);
			bbuf.putInt( codeBytes.length );
			bbuf.put(codeBytes);
			exactLength += (Integer.BYTES + codeBytes.length);
			
			// put valuesMapString, can be null
			assert(valuesMapString != null):"valuesMapString can't be null for active request";
			valuesMapBytes = valuesMapString.getBytes(CHARSET);
			bbuf.putInt( valuesMapBytes.length );
			bbuf.put(valuesMapBytes);
			exactLength += (Integer.BYTES + valuesMapBytes.length);
			break;
			
		case READ_QUERY:
			// put ttl
			bbuf.putInt(ttl);
			exactLength += Integer.BYTES;
			
			// put guid, can't be null, ~100ns
			assert(guid != null):"guid can't be null for read query";
			guidBytes = guid.getBytes(CHARSET);
			bbuf.putInt(guidBytes.length);
			bbuf.put(guidBytes);
			exactLength += (Integer.BYTES + guidBytes.length);
			
			// put field, can't be null, ~100ns
			assert(field != null):"field can't be null for read query";
			fieldBytes = field.getBytes(CHARSET);
			bbuf.putInt(fieldBytes.length);
			bbuf.put(fieldBytes);
			exactLength += (Integer.BYTES + fieldBytes.length);
			
			// put targetGuid, can't be null, ~100ns
			assert(targetGuid != null):"targetGuid can't be null for read query";
			targetGuidBytes = targetGuid.getBytes(CHARSET);
			bbuf.putInt(targetGuidBytes.length);
			bbuf.put(targetGuidBytes);
			exactLength += (Integer.BYTES + targetGuidBytes.length);
			break;
			
		case WRITE_QUERY:
			// put ttl
			bbuf.putInt(ttl);
			exactLength += Integer.BYTES;
			
			// put guid, can't be null, ~100ns
			assert(guid != null):"guid can't be null for read query";
			guidBytes = guid.getBytes(CHARSET);
			bbuf.putInt(guidBytes.length);
			bbuf.put(guidBytes);
			exactLength += (Integer.BYTES + guidBytes.length);
			
			// put field, can't be null, ~100ns
			assert(field != null):"field can't be null for read query";
			fieldBytes = field.getBytes(CHARSET);
			bbuf.putInt(fieldBytes.length);
			bbuf.put(fieldBytes);
			exactLength += (Integer.BYTES + fieldBytes.length);
			
			// put targetGuid, can't be null, ~100ns
			assert(targetGuid != null):"targetGuid can't be null for read query";
			targetGuidBytes = targetGuid.getBytes(CHARSET);
			bbuf.putInt(targetGuidBytes.length);
			bbuf.put(targetGuidBytes);
			exactLength += (Integer.BYTES + targetGuidBytes.length);
			
			// put value
			assert(valuesMapString != null);
			valuesMapBytes = valuesMapString.getBytes(CHARSET);
			bbuf.putInt(valuesMapBytes.length);
			bbuf.put(valuesMapBytes);
			exactLength += (Integer.BYTES + valuesMapBytes.length);
			break;
			
		case RESPONSE:
			valuesMapBytes = (valuesMapString==null)?new byte[0]:valuesMapString.getBytes(CHARSET);
			bbuf.putInt((valuesMapString==null)?0:valuesMapBytes.length);
			bbuf.put(valuesMapBytes);
			exactLength += (Integer.BYTES + valuesMapBytes.length);
			
			byte[] errorBytes = (error==null)? new byte[0]:error.getBytes(CHARSET);
			bbuf.putInt( (error==null)?0:errorBytes.length );
			bbuf.put(errorBytes);
			exactLength += (Integer.BYTES + ((error==null)? 0:errorBytes.length));
			break;
		
		}
		
		byte[] exactBytes = new byte[exactLength];
		bbuf.flip();
		assert (bbuf.remaining() == exactLength) : bbuf.remaining() + " != " + exactLength;
		
		bbuf.get(exactBytes);
		return exactBytes;
	}
	
	/**
	 * @param bytes
	 * @throws UnsupportedEncodingException 
	 * @throws JSONException 
	 */
	public ActiveMessage(byte[] bytes) throws UnsupportedEncodingException, JSONException {
		this(ByteBuffer.wrap(bytes));
	}
	
	/**
	 * @param bbuf
	 * @throws UnsupportedEncodingException 
	 * @throws JSONException 
	 */
	public ActiveMessage(ByteBuffer bbuf) throws UnsupportedEncodingException, JSONException {
		this.type = Type.values()[bbuf.getInt()];	
		this.id = bbuf.getLong();
		int length = 0;
		byte[] guidBytes,fieldBytes,codeBytes,targetGuidBytes,valueBytes,errorBytes;
		
		switch(type){
		case REQUEST:
			ttl = bbuf.getInt();			
			// get guid
			length = bbuf.getInt();
			guidBytes = new byte[length];
			bbuf.get(guidBytes);
			guid = new String(guidBytes, CHARSET);
			
			// get field
			length = bbuf.getInt();
			if(length>0){
				fieldBytes = new byte[length];
				bbuf.get(fieldBytes);
				field = new String(fieldBytes, CHARSET);
			}
			
			// get code
			length = bbuf.getInt();
			codeBytes = new byte[length];
			bbuf.get(codeBytes);
			code = new String(codeBytes, CHARSET);
						
			// get valuesMap
			length = bbuf.getInt();
			valueBytes = new byte[length];
			bbuf.get(valueBytes);
			value = new ValuesMap(new JSONObject(new String(valueBytes, CHARSET)));
			break;
		case READ_QUERY:
			ttl = bbuf.getInt();
			// get guid
			length = bbuf.getInt();
			guidBytes = new byte[length];
			bbuf.get(guidBytes);
			guid = new String(guidBytes, CHARSET);
			
			// get field
			length = bbuf.getInt();
			fieldBytes = new byte[length];
			bbuf.get(fieldBytes);
			field = new String(fieldBytes, CHARSET);
			
			// get targetGuid
			length = bbuf.getInt();
			targetGuidBytes = new byte[length];
			bbuf.get(targetGuidBytes);
			targetGuid = new String(targetGuidBytes, CHARSET);
			break;
			
		case WRITE_QUERY:
			ttl = bbuf.getInt();
			// get guid
			length = bbuf.getInt();
			guidBytes = new byte[length];
			bbuf.get(guidBytes);
			guid = new String(guidBytes, CHARSET);
			
			// get field
			length = bbuf.getInt();
			fieldBytes = new byte[length];
			bbuf.get(fieldBytes);
			field = new String(fieldBytes, CHARSET);
			
			// get targetGuid
			length = bbuf.getInt();
			targetGuidBytes = new byte[length];
			bbuf.get(targetGuidBytes);
			targetGuid = new String(targetGuidBytes, CHARSET);
			
			// get valuesMap
			length = bbuf.getInt();
			valueBytes = new byte[length];
			bbuf.get(valueBytes);
			value = new ValuesMap(new JSONObject(new String(valueBytes, CHARSET)));
			break;
			
		case RESPONSE:
			// get valuesMap
			length = bbuf.getInt();
			if(length>0){
				valueBytes = new byte[length];
				bbuf.get(valueBytes);
				value = new ValuesMap(new JSONObject(new String(valueBytes, CHARSET)));
			}
			
			length = bbuf.getInt();
			if(length>0){
				errorBytes = new byte[length];
				bbuf.get(errorBytes);
				error = new String(errorBytes, CHARSET);
			}
			
			break;
			
		}
	}
	
	@Override
	public String toString(){
		
		return "[id:"+id
				+ ",guid:"+ ((guid != null)?guid:"null")
				+",field:"+((field!=null)?field:"null")
				+",value:"+((value!=null)?value:"null")
				+",error:"+((error!=null)?error:"null")
				+"]";
	}
	
	/**
	 * @param args
	 * @throws JSONException
	 * @throws UnsupportedEncodingException 
	 */
	public static void main(String[] args) throws JSONException, UnsupportedEncodingException{
		
		String guid = "zhaoyu";
		String field = "gao";
		String noop_code = "";
		
		try {
			noop_code = new String(Files.readAllBytes(Paths.get("./scripts/activeCode/noop.js")));

		} catch (IOException e) {
			e.printStackTrace();
		} 
		ValuesMap value = new ValuesMap();
		value.put("string", "hello world");
		
		System.out.println("Test initializing REQUEST message");
		int n = 1000000;
		long t = System.currentTimeMillis();	
		for (int i=0; i<n; i++){
			new ActiveMessage(guid, field, noop_code, value, 0);
		}
		long elapsed = System.currentTimeMillis() - t;		
		System.out.println("It takes "+elapsed+"ms for create 1m REQUEST ActiveMessage, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");
		
		ActiveMessage amsg = new ActiveMessage(guid, field, noop_code, value, 0);
		ActiveMessage rmsg = null;
		
		t = System.currentTimeMillis();
		for (int i=0; i<n; i++){
			amsg.getValue();
		}
		long s = System.currentTimeMillis() - t;
		System.out.println("It takes "+s+"ms for getting a field from the message, and the average latency for each operation is "+(s*1000.0/n)+"us");	
		
		
		t = System.currentTimeMillis();
		for (int i=0; i<n; i++){
			rmsg = new ActiveMessage(amsg.toBytes());
			ActiveMessage resp = new ActiveMessage(rmsg.getId(), rmsg.getValue(), null);
			new ActiveMessage(resp.toBytes());
			//System.out.println(resp+" "+deserialized);
		}
		elapsed = System.currentTimeMillis() - t;
		System.out.println("It takes "+elapsed+"ms for serialize and deserialize 1m REQUEST and RESPONSE ActiveMessage, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");	
		
		int ttl = 1;
		String querierGuid = "zhaoyu gao";
		String queriedGuid = "alvin";
		
		int k = 1000000;
		t = System.currentTimeMillis();
		for (int i=0; i<k; i++){
			rmsg = new ActiveMessage(amsg.toBytes());
			byte[] buf = new ActiveMessage(ttl, querierGuid, field, queriedGuid).toBytes();
			new ActiveMessage(buf);
		}
		elapsed = System.currentTimeMillis() - t;
		System.out.println("It takes "+elapsed+"ms for serialize and deserialize READ_QUERY 1m ActiveMessage, and the average latency for each operation is "+(elapsed*1000.0/k)+"us");	
		
	}

}
