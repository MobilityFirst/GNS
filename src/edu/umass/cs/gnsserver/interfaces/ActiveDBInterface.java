package edu.umass.cs.gnsserver.interfaces;

import java.util.ArrayList;

import org.json.JSONObject;

import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnsserver.gnsapp.GNSApp;
import edu.umass.cs.gnsserver.gnsapp.GNSCommandInternal;
import edu.umass.cs.gnsserver.gnsapp.activegns.ActiveGNSClient;


public interface ActiveDBInterface {


	public JSONObject read(InternalRequestHeader header, String targetGUID,
			String field) throws ClientException, InternalRequestException ;


	@Deprecated
	default JSONObject read(String querierGUID, String targetGUID, String field)
			throws ClientException {
		return null;
	}


	public JSONObject read(InternalRequestHeader header, String targetGUID,
			ArrayList<String> fields) throws ClientException, InternalRequestException;


	@Deprecated
	default void write(String querierGUID, String targetGUID, String field,
			JSONObject valuesMap) throws ClientException {
	}


	public void write(InternalRequestHeader header, String targetGUID,
			String field, JSONObject valuesMap) throws ClientException, InternalRequestException;

}
