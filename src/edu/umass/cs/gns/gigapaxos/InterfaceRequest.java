package edu.umass.cs.gns.gigapaxos;

import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.reconfiguration.RequestParseException;

/**
 * @author V. Arun
 */
public interface InterfaceRequest {
	public static String NO_OP = "NO_OP";

	public IntegerPacketType getRequestType() throws RequestParseException;

	public String getServiceName(); // unique app-level replica-group ID

	public String toString(); // must be explicitly overridden
}
