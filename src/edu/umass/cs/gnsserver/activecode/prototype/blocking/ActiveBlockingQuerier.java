package edu.umass.cs.gnsserver.activecode.prototype.blocking;

import java.io.IOException;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.maxmind.geoip2.record.Location;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveException;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.ACLQuerier;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.DNSQuerier;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Querier;
import jdk.nashorn.api.scripting.ScriptObjectMirror;

/**
 * This class is an implementation of Querier, Querier only contains
 * readGuid and writeGuid method, so the protected methods will not be
 * exposed to the javascript code.
 * @author gaozy
 *
 */
public class ActiveBlockingQuerier implements Querier,ACLQuerier,DNSQuerier {
	private final Channel channel;
	private final ScriptObjectMirror JSON;
	private int currentTTL;
	private final String currentGuid;
	private final long currentID;
	
	/**
	 * @param channel
	 * @param JSON 
	 * @param ttl 
	 * @param guid 
	 * @param id 
	 */
	public ActiveBlockingQuerier(Channel channel, ScriptObjectMirror JSON, int ttl, String guid, long id){
		this.channel = channel;
		this.JSON = JSON;
		this.currentTTL = ttl;
		this.currentGuid = guid;
		this.currentID = id;
	}
	
	
	/**
	 * @param channel
	 */
	public ActiveBlockingQuerier(Channel channel, ScriptObjectMirror JSON){
		this(channel, JSON, 0, null, 0);
	}

	
	/**
	 * @param queriedGuid
	 * @param field
	 * @return ValuesMap the code trying to read
	 * @throws ActiveException
	 */
	@Override
	public ScriptObjectMirror readGuid(String queriedGuid, String field) throws ActiveException{
		if(currentTTL <=0)
			throw new ActiveException(); //"Out of query limit"
		if(queriedGuid==null)
			return readValueFromField(currentGuid, currentGuid, field, currentTTL);
		return readValueFromField(currentGuid, queriedGuid, field, currentTTL);
	}
	
	/**
	 * @param queriedGuid
	 * @param field
	 * @param value
	 * @throws ActiveException
	 */
	@Override
	public void writeGuid(String queriedGuid, String field, ScriptObjectMirror value) throws ActiveException{
		if(currentTTL <=0)
			throw new ActiveException(); //"Out of query limit"
		if(queriedGuid==null)
			writeValueIntoField(currentGuid, currentGuid, field, js2String(value), currentTTL);
		else
			writeValueIntoField(currentGuid, queriedGuid, field, js2String(value), currentTTL);
	}
	
	
	private ScriptObjectMirror readValueFromField(String querierGuid, String queriedGuid, String field, int ttl)
			throws ActiveException {
		ScriptObjectMirror value = null;
		try{
			ActiveMessage am = new ActiveMessage(ttl, querierGuid, field, queriedGuid, currentID);
			channel.sendMessage(am);
			ActiveMessage response = (ActiveMessage) channel.receiveMessage();
			
			if(response == null){
				throw new ActiveException();
			}
			
			if (response.getError() != null){
				throw new ActiveException();
			}
			value = string2JS(response.getValue());
		} catch(IOException e) {
			throw new ActiveException();
		}
		return value;
	}

	private void writeValueIntoField(String querierGuid, String targetGuid, String field, String value, int ttl)
			throws ActiveException {
		
			ActiveMessage am = new ActiveMessage(ttl, querierGuid, field, targetGuid, value, currentID);
			try {
				channel.sendMessage(am);
				ActiveMessage response = (ActiveMessage) channel.receiveMessage();
				
				if(response == null){
					throw new ActiveException();
				}
				if (response.getError() != null){
					throw new ActiveException();
				}
			} catch (IOException e) {
				throw new ActiveException();
			}
	}

	@Override
	public JSONObject lookupUsernameForGuid(String targetGuid) throws ActiveException {
		throw new RuntimeException("unimplemented");
	}

	@Override
	public ScriptObjectMirror getLocations(ScriptObjectMirror ipList) throws ActiveException {
		// TODO Auto-generated method stub
		return null;
	}
	
	@SuppressWarnings("restriction")
	protected ScriptObjectMirror string2JS(String str){
		return (ScriptObjectMirror) JSON.callMember("parse", str);
	}
	
	@SuppressWarnings("restriction")
	protected String js2String(ScriptObjectMirror obj){
		return (String) JSON.callMember("stringify", obj);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args){
		ActiveBlockingQuerier querier = new ActiveBlockingQuerier(null, null);
		int ttl = 1;
		String guid = "Zhaoyu Gao";			
		
		int n = 1000000;		
		long t1 = System.currentTimeMillis();		
		for(int i=0; i<n; i++){
			
		}		
		long elapsed = System.currentTimeMillis() - t1;
		System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");
	}

}
