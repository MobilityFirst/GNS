package edu.umass.cs.gigapaxos;

import java.util.Set;

import edu.umass.cs.nio.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * @author V. Arun
 */
public interface InterfaceApplication {
	/**
	 * @param request
	 * @return True if the request is executed successfully. 
	 */
	public boolean handleRequest(InterfaceRequest request);

	/**
	 * App must support string to InterfaceRequest conversion and back.
	 * Furthermore, the conversion to a string and back must preserve the return
	 * values of all InterfaceRequest methods, i.e.,
	 * (app.getRequest(request.toString())).getRequestType =
	 * request.getRequestType() ... and so on
	 * 
	 * @param stringified
	 * @return InterfaceRequest corresponding to {@code stringified}.
	 * @throws RequestParseException
	 */
	public InterfaceRequest getRequest(String stringified) throws RequestParseException;
	
	/**
	 * @return The set of request types that the application expects to process.
	 */
	public Set<IntegerPacketType> getRequestTypes();
}
