package edu.umass.cs.gnsserver.activecode;

import java.io.IOException;
import java.util.ArrayList;

import org.json.JSONObject;

import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnsserver.gnsapp.activegns.ActiveGNSClient;
import edu.umass.cs.gnsserver.interfaces.ActiveDBInterface;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

/**
 *
 */
public class ActiveCodeDB implements ActiveDBInterface {
	
	ActiveGNSClient client;

	/**
	 * 
	 */
	public ActiveCodeDB(){
		try {
			this.client = new ActiveGNSClient();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public JSONObject read(InternalRequestHeader header, String targetGUID, String field) 
			throws InternalRequestException, ClientException {
		JSONObject obj = client.read(header, targetGUID, field);
		return obj;
	}

	@Override
	public JSONObject read(InternalRequestHeader header, String targetGUID, ArrayList<String> fields) 
			throws InternalRequestException, ClientException{
		return client.read(header, targetGUID, fields);
	}

	@Override
	public void write(InternalRequestHeader header, String targetGUID, String field, JSONObject valuesMap) 
			throws InternalRequestException, ClientException{
			client.write(header, targetGUID, field, valuesMap);		
	}
}
