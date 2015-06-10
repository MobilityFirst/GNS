package edu.umass.cs.gigapaxos;

import edu.umass.cs.nio.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * @author V. Arun
 */
public interface InterfaceRequest {
	/**
	 * A string representing a special no-op request.
	 */
	public static String NO_OP = "NO_OP";

	/**
	 * @return The IntegerPacketType type corresponding to this request.
	 * @throws RequestParseException
	 */
	public IntegerPacketType getRequestType() throws RequestParseException;

	/**
	 * @return Returns the unique app-level replica-group ID.
	 */
	public String getServiceName(); 

	/**
	 * Serializes the request to a String. The default toString() method 
	 * must be overridden.
	 * 
	 * @return Returns this request serialized as a String.
	 */
	public String toString(); // must be explicitly overridden
}
