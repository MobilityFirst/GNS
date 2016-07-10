package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsserver.activecode.prototype.interfaces.ActiveChannel;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Querier;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * This class is an implementation of Querier, Querier only contains
 * readGuid and writeGuid method, so the protected methods will not be
 * exposed to the javascript code.
 * @author gaozy
 *
 */
public class ActiveQuerier implements Querier {
	private ActiveChannel channel;
	private final byte[] buffer = new byte[ActiveWorker.bufferSize];
	private int currentTTL = 0;
	private String currentGuid = "";
	
	/**
	 * @param channel
	 */
	public ActiveQuerier(ActiveChannel channel){
		this.channel = channel;
	}
		
	/**
	 * @param guid
	 * @param ttl
	 */
	protected void resetQuerier(String guid, int ttl){
		this.currentGuid = guid;
		this.currentTTL = ttl;
	}
	
	/**
	 * @param queriedGuid
	 * @param field
	 * @return ValuesMap the code trying to read
	 * @throws ActiveException
	 */
	@Override
	public ValuesMap readGuid(String queriedGuid, String field) throws ActiveException{
		return readValueFromField(currentGuid, queriedGuid, field, currentTTL--);
	}
	
	/**
	 * @param queriedGuid
	 * @param field
	 * @param value
	 * @return true if the write succeeds, false otherwise
	 * @throws ActiveException
	 */
	@Override
	public boolean writeGuid(String queriedGuid, String field, Object value) throws ActiveException{
		return writeValueIntoField(currentGuid, queriedGuid, field, value, currentTTL--);
	}
	
	
	private ValuesMap readValueFromField(String querierGuid, String queriedGuid, String field, int ttl)
			throws ActiveException {
		ValuesMap value = null;
		try{
			ActiveMessage am = new ActiveMessage(ttl, querierGuid, field, queriedGuid);
			//System.out.println("Send back query");
			byte[] buf = am.toBytes();
			channel.write(buf, 0, buf.length);
			int length = channel.read(buffer);
			if(length >0){
				value = new ActiveMessage(buffer).getValue();				
			}
			Arrays.fill(buffer, (byte) 0);
		} catch(UnsupportedEncodingException | JSONException e) {
			throw new ActiveException();
		}
		return value;
	}

	private boolean writeValueIntoField(String querierGuid, String queriedGuid, String field, Object value, int ttl)
			throws ActiveException {
		boolean wSuccess = false;
		try{
			ValuesMap map = new ValuesMap();
			map.put(field, value);
			ActiveMessage am = new ActiveMessage(ttl, querierGuid, queriedGuid, field, map);
			byte[] buf = am.toBytes();
			channel.write(buf, 0, buf.length);
			int length = channel.read(buffer);
			map = new ValuesMap(new JSONObject(new String(buffer,0,length)));
			if(map.getBoolean(field)){
				wSuccess = true;
			}
		}catch(Exception e){
			throw new ActiveException();
		}
		return wSuccess;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args){
		ActiveQuerier querier = new ActiveQuerier(null);		
		int ttl = 1;
		String guid = "Zhaoyu Gao";		
		
		int n = 1000000;		
		long t1 = System.currentTimeMillis();		
		for(int i=0; i<n; i++){
			querier.resetQuerier(guid, ttl);
		}		
		long elapsed = System.currentTimeMillis() - t1;
		System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");
	}
}
