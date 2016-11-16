package edu.umass.cs.gnsserver.interfaces;

import java.util.ArrayList;

import org.json.JSONObject;

import edu.umass.cs.gnscommon.exceptions.client.ClientException;

/**
 * @author arun
 * 
 *         This interface is meant to be used by JS to issue read/write
 *         operations, so an active request worker is expected to pass an
 *         instance of this interface to JS via the script engine.
 * 
 *         This interface currently only supports simple field read and write
 *         queries. It currently does not support create, delete, select, or
 *         {@link edu.umass.cs.gnscommon.CommandType.Type#OTHER} commands or for
 *         that matter other
 *         {@link edu.umass.cs.gnscommon.CommandType.Type#READ} or
 *         {@link edu.umass.cs.gnscommon.CommandType.Type#UPDATE} queries, but
 *         some subset of these other query types may be supported in future
 *         versions.
 * 
 * 
 */
public interface ActiveGNSInterface {
	/**
	 * This method will return the value of {@code targetGUID.field} and is
	 * meant to be used by JS running in the script engine.
	 * 
	 * This method can be used to read the entire GUID record (subject to ACL
	 * checks) by specifying {@code field} as
	 * {@link GNSCommandProtocol#ALL_FIELDS}.
	 * 
	 * @param targetGUID
	 * @param field
	 *
	 * @return JSONObject representation of guid.field.
	 * @throws ClientException
	 */
	public JSONObject read(String targetGUID, String field)
			throws ClientException;

	/**
	 * @param targetGUID
	 * @param fields
	 * @return JSONObject representation of values of {@code fields} of
	 *         {@code targetGUID}.
	 */
	public JSONObject read(String targetGUID, ArrayList<String> fields);

	/**
	 * @param targetGUID
	 * @param field
	 *            The queried (updated) field.
	 * @param valuesMap
	 *            The value being assigned to {@code field}.
	 * @throws ClientException
	 */
	public void write(String targetGUID, String field, JSONObject valuesMap)
			throws ClientException;
}
