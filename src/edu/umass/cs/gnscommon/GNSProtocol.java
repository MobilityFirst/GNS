package edu.umass.cs.gnscommon;

import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;


public enum GNSProtocol {
  //
  // Response codes
  //
  //
  // Response codes
  //

  OK_RESPONSE("+OK+"),

  BAD_RESPONSE("+NO+"),

  UNSPECIFIED_ERROR("+GENERICERROR+"),

  BAD_SIGNATURE("+BAD_SIGNATURE+"),

  ACCESS_DENIED("+ACCESS_DENIED+"),

  STALE_COMMMAND("+STALE_COMMMAND+"),

  OPERATION_NOT_SUPPORTED("+OPERATIONNOTSUPPORTED+"),

  QUERY_PROCESSING_ERROR("+QUERYPROCESSINGERROR+"),

  VERIFICATION_ERROR("+VERIFICATIONERROR+"),

  ALREADY_VERIFIED_EXCEPTION("+ALREADYVERIFIED+"),

  REMOTE_QUERY_EXCEPTION("+REMOTEQUERY+"),

  BAD_ACCESSOR_GUID("+BADACCESSORGUID+"),

  BAD_GUID("+BADGUID+"),

  BAD_ACCOUNT("+BADACCOUNT+"),

  BAD_ALIAS("+BADALIAS+"),

  BAD_ACL_TYPE("+BADACLTYPE+"),

  FIELD_NOT_FOUND("+FIELDNOTFOUND+"),

  DUPLICATE_GUID("+DUPLICATEGUID+"),

  DUPLICATE_FIELD("+DUPLICATEFIELD+"),

  DUPLICATE_NAME("+DUPLICATENAME+"),

  JSON_PARSE_ERROR("+JSONPARSEERROR+"),

  TOO_MANY_ALIASES("+TOMANYALIASES+"),

  TOO_MANY_GUIDS("+TOMANYGUIDS+"),

  UPDATE_ERROR("+UPDATEERROR+"),

  DATABASE_OPERATION_ERROR("+DATABASEOPERROR+"),

  TIMEOUT("+TIMEOUT+"),

  ACTIVE_REPLICA_EXCEPTION(ClientReconfigurationPacket.ResponseCodes.ACTIVE_REPLICA_EXCEPTION.toString()),
  //
  // End of Response codes
  //

  NULL_RESPONSE("+NULL+"),

  RSA_ALGORITHM("RSA"),

  SIGNATURE_ALGORITHM("SHA1withRSA"),

  DIGEST_ALGORITHM("SHA1"),

  SECRET_KEY_ALGORITHM("DESede"),

  // FIXME: The reason why we use ISO-8859-1 should be more throughly documented here.
  CHARSET("ISO-8859-1"),
  //
  // Argument fields in commands.
  //

  GUID("guid"),

  NAME("name"),

  NAMES("names"),

  ACCOUNT_GUID("accountGuid"),

  READER("reader"),

  WRITER("writer"),

  ACCESSER("accesser"),

  FIELD("field"),

  FIELDS("fields"),

  VALUE("value"),

  OLD_VALUE("oldvalue"),

  USER_JSON("userjson"),

  ARGUMENT("argument"),

  N("n"),

  MEMBER("member"),

  MEMBERS("members"),

  ACL_TYPE("aclType"),

  PUBLIC_KEY("publickey"),

  PUBLIC_KEYS("publickeys"),

  PASSWORD("password"),

  CODE("code"),

  SIGNATURE("signature"),
  // select commands

  WITHIN("within"),

  NEAR("near"),

  MAX_DISTANCE("maxDistance"),

  QUERY("query"),

  INTERVAL("interval"),

  ENTIRE_RECORD("+ALL+"),

  ALL_GUIDS("+ALL+"),

  EVERYONE("+ALL+"),
  // admin commands

  LOG_LEVEL("level"),

  GUIDCNT("guidCnt"),
  //
  // Command packet fields
  //

  TIMESTAMP("timestamp"),

  NONCE("seqnum"),

  PASSKEY("passkey"),

  SIGNATUREFULLMESSAGE("_signatureFullMessage_"),
  // Special fields for ACL

  GROUP_ACL("+GROUP_ACL+"),
  // Field names in guid record JSON Object

  GUID_RECORD_PUBLICKEY("publickey"),

  GUID_RECORD_NAME("name"),

  GUID_RECORD_GUID("guid"),

  GUID_RECORD_TYPE("type"),

  GUID_RECORD_CREATED("created"),

  GUID_RECORD_UPDATED("updated"),

  GUID_RECORD_TAGS("tags"),
  // Field names in account record JSON Object

  ACCOUNT_RECORD_VERIFIED("verified"),

  ACCOUNT_RECORD_GUIDS("guids"),

  ACCOUNT_RECORD_GUID("guid"),

  ACCOUNT_RECORD_USERNAME("username"),

  ACCOUNT_RECORD_CREATED("created"),

  ACCOUNT_RECORD_UPDATED("updated"),

  ACCOUNT_RECORD_TYPE("type"),

  ACCOUNT_RECORD_PASSWORD("password"),

  ACCOUNT_RECORD_ALIASES("aliases"),
  // Blessed field names

  LOCATION_FIELD_NAME("geoLocation"),

  LOCATION_FIELD_NAME_2D_SPHERE("geoLocationCurrent"),

  IPADDRESS_FIELD_NAME("netAddress"),

  COMMAND_INT("COMMANDINT"),

  COMMANDNAME("COMMANDNAME"),

  FORCE_COORDINATE_READS("COORDREAD"),

  AC_ACTION("acAction"),

  AC_CODE("acCode"),
  //

  INTERNAL_PREFIX("_GNS_"),

  NEWLINE(System.getProperty("line.separator")),
  //
  // Misc
  //
  //

  ORIGINATING_GUID("OGUID"),

  ORIGINATING_QID("OQID"),

  REQUEST_TTL("QTTL"),
  

  QUERIER_GUID ("QGUID"),

  

  INTERNAL_PROOF ("IPROOF"),
  

  INTERNAL_QUERIER("IQUERIER"),


  REQUEST_ID("QID"),

  RETURN_VALUE("RVAL"),

  COMMAND_QUERY("QVAL"),

  SERVICE_NAME("NAME"),

  UNKNOWN_NAME("unknown"),

  ERROR_CODE("ECODE"),

  INTERNAL_REQUEST_EXCEPTION("+INTERNAL_REQUEST_EXCEPTION+"),

  COORD1("COORD1"),;

  final String label;

  GNSProtocol(String label) {
    this.label = label != null ? label : this.name();
  }

  @Override
  public String toString() {
    return this.label;
  }
  
  private static String generateSwiftConstants() {
    StringBuilder result = new StringBuilder();
    for (GNSProtocol entry : GNSProtocol.values()) {
      result.append("    public static let ");
      result.append(entry.name().toUpperCase());
      result.append("\t\t\t\t = ");
      result.append("\"");
      result.append(entry.toString());
      result.append("\"\n");
    }
    return result.toString();
  }
  
  public static void main(String args[]) {
    System.out.println(generateSwiftConstants());
  }
}
