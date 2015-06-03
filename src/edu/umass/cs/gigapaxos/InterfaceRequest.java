package edu.umass.cs.gigapaxos;

import edu.umass.cs.nio.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * @author V. Arun
 */
public interface InterfaceRequest {
	public static String NO_OP = "NO_OP";

	public IntegerPacketType getRequestType() throws RequestParseException;

	public String getServiceName(); // unique app-level replica-group ID

	public String toString(); // must be explicitly overridden
}
