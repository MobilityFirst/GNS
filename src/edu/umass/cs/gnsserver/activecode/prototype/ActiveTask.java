package edu.umass.cs.gnsserver.activecode.prototype;

import java.util.concurrent.Callable;

import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author gaozy
 *
 */
public class ActiveTask implements Callable<ValuesMap> {
	ActiveClient client;
	String guid;
	String field;
	String code;
	ValuesMap value;
	int ttl;
	
	ActiveTask(ActiveClient client, String guid, String field, String code, ValuesMap value, int ttl){
		this.client = client;
		this.guid = guid;
		this.field = field;
		this.code = code;
		this.value = value;
		this.ttl = ttl;
	}
	
	@Override
	public ValuesMap call() throws Exception {	
		return client.runCode(guid, field, code, value, ttl);
	}

}
