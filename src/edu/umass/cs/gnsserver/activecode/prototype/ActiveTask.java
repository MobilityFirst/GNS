package edu.umass.cs.gnsserver.activecode.prototype;

import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Client;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import org.json.JSONObject;

import java.util.concurrent.Callable;


public class ActiveTask implements Callable<JSONObject> {
	Client client;
	String guid;
	String accessor;
	String code;
	ValuesMap value;
	int ttl;
	
	ActiveTask(Client client, String guid, String accessor, String code, ValuesMap value, int ttl){
		this.client = client;
		this.guid = guid;
		this.accessor = accessor;
		this.code = code;
		this.value = value;
		this.ttl = ttl;
	}
	
	@Override
	public JSONObject call() throws Exception {
		return client.runCode(null, guid, accessor, code, value, ttl, 1000);
	}

}
