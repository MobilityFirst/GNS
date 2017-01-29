package edu.umass.cs.gnsserver.interfaces;

import java.util.ArrayList;

import org.json.JSONObject;

import edu.umass.cs.gnscommon.exceptions.client.ClientException;


public interface ActiveGNSInterface {

	public JSONObject read(String targetGUID, String field)
			throws ClientException;


	public JSONObject read(String targetGUID, ArrayList<String> fields);


	public void write(String targetGUID, String field, JSONObject valuesMap)
			throws ClientException;
}
