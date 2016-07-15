package edu.umass.cs.gnsserver.activecode.prototype.multithreading;

import java.io.IOException;

import org.json.JSONException;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveException;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Querier;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * This a querier with a monitor to let it run in a multithreaded worker.
 * @author gaozy
 *
 */
public class MultiThreadActiveQuerier implements Querier{
	
	private Channel channel;
	private int currentTTL;
	private String currentGuid;
	
	protected MultiThreadActiveQuerier(Channel channel, int ttl, String guid){
		this.channel = channel;
		this.currentTTL = ttl;
		this.currentGuid = guid;
	}
	
	/**
	 * @param channel
	 */
	public MultiThreadActiveQuerier(Channel channel){
		this(channel, 0, null);
	}
	
	/**
	 * @param guid
	 * @param ttl
	 */
	protected void resetQuerier(String guid, int ttl){
		this.currentGuid = guid;
		this.currentTTL = ttl;
	}
	
	@Override
	public ValuesMap readGuid(String queriedGuid, String field) throws ActiveException {
		if(currentTTL <=0)
			throw new ActiveException(); //"Out of query limit"
		if(queriedGuid==null)
			return readValueFromField(currentGuid, currentGuid, field, currentTTL--);
		return readValueFromField(currentGuid, queriedGuid, field, currentTTL--);
	}

	@Override
	public void writeGuid(String queriedGuid, String field, Object value) throws ActiveException {
		if(currentTTL <=0)
			throw new ActiveException(); //"Out of query limit"
		if(queriedGuid==null)
			writeValueIntoField(currentGuid, currentGuid, field, value, currentTTL--);
		else
			writeValueIntoField(currentGuid, queriedGuid, field, value, currentTTL--);
	}
	
	
	private ValuesMap readValueFromField(String querierGuid, String queriedGuid, String field, int ttl)
			throws ActiveException {
		ValuesMap value = null;
		try{
			ActiveMessage am = new ActiveMessage(ttl, querierGuid, field, queriedGuid);
			channel.sendMessage(am);
			ActiveMessage response = (ActiveMessage) channel.receiveMessage();
			value = response.getValue();
		} catch(IOException e) {
			throw new ActiveException();
		}
		return value;
	}

	private void writeValueIntoField(String querierGuid, String queriedGuid, String field, Object value, int ttl)
			throws ActiveException {
		
			ValuesMap map = new ValuesMap();
			try {
				map.put(field, value);
			} catch (JSONException e) {
				e.printStackTrace();
				throw new ActiveException();
			}
			ActiveMessage am = new ActiveMessage(ttl, querierGuid, queriedGuid, field, map);
			try {
				channel.sendMessage(am);
				ActiveMessage response = (ActiveMessage) channel.receiveMessage();
				if (response.getError() != null){
					throw new ActiveException();
				}
			} catch (IOException e) {
				throw new ActiveException();
			}
	}
	
}
