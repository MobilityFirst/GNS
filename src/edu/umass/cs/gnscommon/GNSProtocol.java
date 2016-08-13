package edu.umass.cs.gnscommon;

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

	;

	final String label;

	GNSProtocol(String label) {
		this.label = label!=null ? label : this.name();
	}
	
	public String toString() {
		return this.label;
	}
}
