package edu.umass.cs.gns.reconfiguration;

import edu.umass.cs.gns.nio.IntegerPacketType;

/**
@author V. Arun
 */
public interface InterfaceRequest {
	//public boolean isStop();
	public IntegerPacketType getRequestType() throws RequestParseException; // meant for packet type
	public String getServiceName(); // meant for GUID
        @Override
	public String toString(); // must be explicitly overridden
}
