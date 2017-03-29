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
 * @author westy
 */
public class ActiveCodeDB implements ActiveDBInterface {
	
	private ActiveGNSClient client;

	/**
	 * Create the active code.
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
		return client.read(header, targetGUID, field);
	}

	@Override
	public JSONObject read(InternalRequestHeader header, String targetGUID, ArrayList<String> fields) 
			throws InternalRequestException, ClientException{
		return client.read(header, targetGUID, fields);
	}

	@Override
	public void write(InternalRequestHeader header, String targetGUID, String field, JSONObject json) 
			throws InternalRequestException, ClientException{
			client.write(header, targetGUID, field, json);		
	}
}
