package edu.umass.cs.gnscommon;

import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;

/**
 * @author arun
 * 
 *         This enum is the set of all GNS response codes. This enum will
 *         supercede all entries in GNSCommandProtocol. The string values are
 *         exactly copied over currently from GNSCommandProtocol.
 * 
 *         FIXME: The numbers, categorization, and documentation of these codes
 *         needs work.
 *
 */
public enum ResponseCodes {

	/**
	 * The standard good case.
	 */
	OK(200, "OK+", false, "Generic success response code"),
	/**
	 * Null responses are not exception cases.
	 */
	NULL_RESPONSE(300, "+NULL+", false),
	/**
	 * 
	 */
	BAD_RESPONSE(400, "+NO+", true),
	/**
	 * 
	 */
	BAD_SIGNATURE(401, "+BAD_SIGNATURE+", true),
	/**
	 * 
	 */
	ACCESS_DENIED(402, "+ACCESS_DENIED+", true),
	/**
	 * 
	 */
	STALE_COMMMAND(403, "+STALE_COMMMAND+", true),
	/**
	 * 
	 */
	OPERATION_NOT_SUPPORTED(404, "+OPERATIONNOTSUPPORTED+", true),
	/**
	 * 
	 */
	QUERY_PROCESSING_ERROR(405, "+QUERYPROCESSINGERROR+", true),
	/**
	 * 
	 */
	VERIFICATION_ERROR(406, "+VERIFICATIONERROR+", true),
	/**
	 * 
	 */
	NO_ACTION_FOUND(407, "+NOACTIONFOUND+", true),
	/**
	 * 
	 */
	BAD_ACCESSOR_GUID(408, "+BADACCESSORGUID+", true),
	/**
	 * 
	 */
	BAD_GUID(409, "+BADGUID+", true),
	/**
	 * 
	 */
	BAD_ACCOUNT(410, "+BADACCOUNT+", true),
	/**
	 * 
	 */
	BAD_USER(411, "+BADUSER+", true),
	/**
	 * 
	 */
	BAD_GROUP(412, "+BADGROUP+", true),
	/**
	 * 
	 */
	BAD_FIELD(413, "+BADFIELD+", true),
	/**
	 * 
	 */
	BAD_ALIAS(414, "+BADALIAS+", true),
	/**
	 * 
	 */
	BAD_ACL_TYPE(415, "+BADACLTYPE+", true),
	/**
	 * 
	 */
	FIELD_NOT_FOUND(416, "+FIELDNOTFOUND+", true),

	/**
	 * 
	 */
	DUPLICATE_USER(440, "+DUPLICATEUSER+", true),
	/**
	 * 
	 */
	DUPLICATE_GUID(441, "+DUPLICATEGUID+", true),
	/**
	 * 
	 */
	DUPLICATE_GROUP(442, "+DUPLICATEGROUP+", true),
	/**
	 * 
	 */
	DUPLICATE_FIELD(443, "+DUPLICATEFIELD+", true),
	/**
	 * 
	 */
	DUPLICATE_NAME(444, "+DUPLICATENAME+", true),
	/**
	 * Corresponds to gigapaxos duplicate name creation error.
	 */
	DUPLICATE_ERROR(445, "+"
			+ ClientReconfigurationPacket.ResponseCodes.DUPLICATE_ERROR
			.toString() + "+", true),

			/**
			 * JSONException while parsing.
			 */
			JSON_PARSE_ERROR(450, "+JSONPARSEERROR+", true),
			/**
			 * 
			 */
			TOO_MANY_ALIASES(460, "+TOMANYALIASES+", true),
			/**
			 * 
			 */
			TOO_MANY_GUIDS(461, "+TOMANYGUIDS+", true),

			/**
			 * 
			 */
			UPDATE_ERROR(470, "+UPDATEERROR+", true),
			/**
			 * 
			 */
			UPDATE_TIMEOUT(471, "+UPDATETIMEOUT+", true),
			/**
			 * 
			 */
			SELECTERROR(480, "+SELECTERROR+", true),
			/**
			 * 
			 */
			
			GENERIC_ERROR(481, "+GENERICERROR+", true),
			/**
			 * 
			 */
			FAIL_ACTIVE_NAMESERVER(482, "+FAIL_ACTIVE+", true),
			/**
			 * 
			 */
			INVALID_ACTIVE_NAMESERVER(483, "+INVALID_ACTIVE+", true),
			/**
			 * Corresponds to gigapaxos' active replica error.
			 */
			NO_ACTIVE_REPLICAS(484,
					ReconfigurationPacket.PacketType.ACTIVE_REPLICA_ERROR.toString(),
					true),
					/**
					 * 
					 */
					TIMEOUT(485, "+TIMEOUT+", true), ;

	int number; // must be unique
	String label; // must be unique
	boolean exceptional; // whether this response code represents an exceptional
	// condition

	String description; // generic description, same as documentation

	ResponseCodes(int number, String label, boolean exceptional) {
		this(number, label, exceptional, "");
	}

	ResponseCodes(int number, String label, boolean exceptional,
			String description) {
		this.number = number;
		this.label = label;
		this.exceptional = exceptional;
		this.description = description;
	}
}
