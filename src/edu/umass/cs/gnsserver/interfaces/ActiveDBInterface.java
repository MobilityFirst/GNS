package edu.umass.cs.gnsserver.interfaces;

import java.util.ArrayList;

import org.json.JSONObject;

import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnsserver.gnsapp.GNSApp;
import edu.umass.cs.gnsserver.gnsapp.GNSCommandInternal;
import edu.umass.cs.gnsserver.gnsapp.activegns.ActiveGNSClient;

/**
 * @author arun
 *
 *         The methods in this interface are meant to be invoked by the worker
 *         process encapsulating the active code script engine. They must not be
 *         directly invokable by JS, so {@code ActiveDBInterface} instances must
 *         not be passed to the script engine.
 */
public interface ActiveDBInterface {

	/**
	 * This method returns the value of {@code targetGUID.field} and is meant to
	 * be used by the worker process encapsulating the active code script
	 * engine.
	 * 
	 * This method can be used to read the entire GUID record (subject to ACL
	 * checks) by specifying {@code field} as
	 * {@link GNSCommandProtocol#ALL_FIELDS}.
	 * 
	 * The implementer is expected to implement this method and others in this
	 * class using {@link ActiveGNSClient} and
	 * {@link GNSCommandInternal#fieldRead(String, String, InternalRequestHeader)}
	 * to construct and send server-internal commands with {@code querierGUID}
	 * set to the originating GUID for the active request chain being processed.
	 * 
	 * A {@link ClientException} means that the implementer may (optionally)
	 * retry the request but an {@link InternalRequestException} must be treated
	 * as an error and it is the implementer's responsibility to ensure that it
	 * is thrown in the thread invoking
	 * {@link GNSApp#execute(edu.umass.cs.gigapaxos.interfaces.Request, boolean)}
	 * .
	 * 
	 * 
	 * @param header
	 *            The header information for the current active active request
	 *            in the chain.
	 * 
	 * @param targetGUID
	 *            The queried GUID.
	 * 
	 * @param field
	 *            The queried field.
	 *
	 * @return JSONObject representation of guid.field.
	 * @throws ClientException
	 * @throws InternalRequestException
	 */
	public JSONObject read(InternalRequestHeader header, String targetGUID,
			String field) throws ClientException, InternalRequestException ;

	/**
	 * @param querierGUID
	 * @param targetGUID
	 * @param field
	 * @return Disabled.
	 * @throws ClientException
	 */
	@Deprecated
	default JSONObject read(String querierGUID, String targetGUID, String field)
			throws ClientException {
		return null;
	}

	/**
	 *  Refer {@link #read(InternalRequestHeader, String, String)} for
	 * implementation guidelines
	 * 
	 * @param header
	 * @param targetGUID
	 * @param fields
	 * @return JSONObject representation of values of {@code fields} of
	 *         {@code targetGUID}.
	 * @throws ClientException 
	 * @throws InternalRequestException 
	 */
	public JSONObject read(InternalRequestHeader header, String targetGUID,
			ArrayList<String> fields) throws ClientException, InternalRequestException;

	/**
	 * @param querierGUID
	 * @param targetGUID
	 * @param field
	 * @param valuesMap
	 * @throws ClientException
	 */
	@Deprecated
	default void write(String querierGUID, String targetGUID, String field,
			JSONObject valuesMap) throws ClientException {
	}

	/**
	 * Refer {@link #read(InternalRequestHeader, String, String)} for
	 * implementation guidelines.
	 * 
	 * @param header
	 * 
	 * @param targetGUID
	 * @param field
	 * @param valuesMap
	 * @throws ClientException
	 * @throws InternalRequestException 
	 */
	public void write(InternalRequestHeader header, String targetGUID,
			String field, JSONObject valuesMap) throws ClientException, InternalRequestException;

}
