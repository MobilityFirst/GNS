package edu.umass.cs.gnscommon;

import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.packets.ResponsePacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.gigapaxos.interfaces.Request;
/**
 * @author arun
 *
 *         An enum class for all the constants used by GNS wireline protocol.
 *         Most of the constants in {@link GNSCommandProtocol} will be migrated
 *         to this class.
 */
public enum GNSProtocol {

	/**
	 * The key for the querier GUID that originated the request chain. This is
	 * the GUID to which all resource usage is charged. This GUID is needed to
	 * enforce ACL checks but not necessarily signature checks as is the case
	 * with internal requests as well as active requests. The request chain here
	 * as well as in the following two enums may refer to a chain of requests
	 * spawned either by active code or simply in the course of regular requests
	 * that transitively spawn other requests.
	 */
	ORIGINATING_GUID("OGUID"),

	/**
	 * The key for the request ID corresponding to the request that originated
	 * the request chain.
	 */
	ORIGINATING_QID("OQID"),

	/**
	 * The number of hops in the request chain remaining before the request
	 * expires.
	 */
	REQUEST_TTL("QTTL"),

	/**
	 * Long client request ID in every {@link CommandPacket}.
	 */
	REQUEST_ID("QID"),

	/**
	 * String return value carried in every {@link ResponsePacket}.
	 */
	RETURN_VALUE("RVAL"),

	/**
	 * The query carried in every {@link CommandPacket}.
	 */
	QUERY("QVAL"),

	/**
	 * Name or HRN or GUID, whatever is used in {@link Request#getServiceName()}.
	 */
	SERVICE_NAME("NAME"),
	/**
	 * The name carried in requests not directed to any particular GUID.
	 */
	UNKNOWN_NAME("unknown"), 
	
	/**
	 * Error code carried in {@link ResponsePacket}.
	 */
	ERROR_CODE("ECODE"), 
	
	/**
	 * Internal request exception message string.
	 */
	INTERNAL_REQUEST_EXCEPTION("+INTERNAL_REQUEST_EXCEPTION+"),
	
	/**
	 * Whether an internal request was previously coordinated (at most once).
	 */
	COORD1("COORD1"),

	;

	final String label;

	GNSProtocol(String label) {
		this.label = label != null ? label : this.name();
	}

	public String toString() {
		return this.label;
	}
}
