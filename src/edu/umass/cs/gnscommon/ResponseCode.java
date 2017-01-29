
package edu.umass.cs.gnscommon;

import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;


public enum ResponseCode implements Serializable {

  NO_ERROR(200, GNSProtocol.OK_RESPONSE.toString(), ResponseCodeType.NORMAL),

  UNSPECIFIED_ERROR(1, GNSProtocol.UNSPECIFIED_ERROR.toString(), ResponseCodeType.ERROR),

  FIELD_NOT_FOUND_ERROR(2, GNSProtocol.FIELD_NOT_FOUND.toString(), ResponseCodeType.ERROR),
  // The following three are access or signature errors

  SIGNATURE_ERROR(5, GNSProtocol.BAD_SIGNATURE.toString(), ResponseCodeType.ERROR),

  STALE_COMMAND_VALUE(6, GNSProtocol.STALE_COMMMAND.toString(), ResponseCodeType.ERROR),

  ACCESS_ERROR(7, GNSProtocol.ACCESS_DENIED.toString(), ResponseCodeType.ERROR),

  BAD_GUID_ERROR(8, GNSProtocol.BAD_GUID.toString(), ResponseCodeType.ERROR),

  BAD_ACCESSOR_ERROR(9, GNSProtocol.BAD_ACCESSOR_GUID.toString(), ResponseCodeType.ERROR),

  VERIFICATION_ERROR(10, GNSProtocol.VERIFICATION_ERROR.toString(), ResponseCodeType.ERROR),

  BAD_ACCOUNT_ERROR(11, GNSProtocol.BAD_ACCOUNT.toString(), ResponseCodeType.ERROR),

  OPERATION_NOT_SUPPORTED(404, GNSProtocol.OPERATION_NOT_SUPPORTED.toString(),
          ResponseCodeType.ERROR),


  ALREADY_VERIFIED_EXCEPTION(12, GNSProtocol.ALREADY_VERIFIED_EXCEPTION.toString(), ResponseCodeType.EXCEPTION),

  DUPLICATE_ID_EXCEPTION(14,
          ClientReconfigurationPacket.ResponseCodes.DUPLICATE_ERROR
          .toString(), ResponseCodeType.EXCEPTION),
          

  DUPLICATE_FIELD_EXCEPTION(15, GNSProtocol.DUPLICATE_FIELD.toString(),
          ResponseCodeType.EXCEPTION),

  NONEXISTENT_NAME_EXCEPTION(16,
          ClientReconfigurationPacket.ResponseCodes.NONEXISTENT_NAME_ERROR
          .toString(), ResponseCodeType.EXCEPTION),

  ACTIVE_REPLICA_EXCEPTION(17,
          ClientReconfigurationPacket.ResponseCodes.ACTIVE_REPLICA_EXCEPTION
          .toString(), ResponseCodeType.EXCEPTION),

  BAD_ALIAS_EXCEPTION(18, GNSProtocol.BAD_ALIAS.toString(), ResponseCodeType.EXCEPTION),

  BAD_ACL_TYPE_ERROR(19, GNSProtocol.BAD_ACL_TYPE.toString(), ResponseCodeType.ERROR),

  FIELD_NOT_FOUND_EXCEPTION(20, GNSProtocol.FIELD_NOT_FOUND.toString(), ResponseCodeType.EXCEPTION),

//  DUPLICATE_GUID_EXCEPTION(21, GNSProtocol.DUPLICATE_GUID.toString(), ResponseCodeType.EXCEPTION),

//  DUPLICATE_NAME_EXCEPTION(22, GNSProtocol.DUPLICATE_NAME.toString(), ResponseCodeType.EXCEPTION),

  JSON_PARSE_ERROR(23, GNSProtocol.JSON_PARSE_ERROR.toString(), ResponseCodeType.ERROR),

  TOO_MANY_ALIASES_EXCEPTION(24, GNSProtocol.TOO_MANY_ALIASES.toString(), ResponseCodeType.EXCEPTION),

  TOO_MANY_GUIDS_EXCEPTION(25, GNSProtocol.TOO_MANY_GUIDS.toString(), ResponseCodeType.EXCEPTION),

  UPDATE_ERROR(26, GNSProtocol.UPDATE_ERROR.toString(), ResponseCodeType.ERROR),

  DATABASE_OPERATION_ERROR(27, GNSProtocol.DATABASE_OPERATION_ERROR.toString(), ResponseCodeType.ERROR),
  

  CONFLICTING_GUID_EXCEPTION(28, "Conflicting GUID", ResponseCodeType.ERROR),


  CONFLICTING_HRN_EXCEPTION(29, "Conflicting HRN", ResponseCodeType.ERROR),


  RECONFIGURATION_EXCEPTION(30, "Reconfiguration exeption", ResponseCodeType.EXCEPTION),


  QUERY_PROCESSING_ERROR(405, GNSProtocol.QUERY_PROCESSING_ERROR.toString(), ResponseCodeType.ERROR),

  TIMEOUT(408, GNSProtocol.TIMEOUT.toString(), ResponseCodeType.EXCEPTION),

  REMOTE_QUERY_EXCEPTION(410, GNSProtocol.REMOTE_QUERY_EXCEPTION.toString(), ResponseCodeType.EXCEPTION),

  INTERNAL_REQUEST_EXCEPTION(411, GNSProtocol.INTERNAL_REQUEST_EXCEPTION
          .toString(), ResponseCodeType.EXCEPTION),


          IO_EXCEPTION(412, IOException.class.getSimpleName(),
        		  ResponseCodeType.EXCEPTION)

        ;

  // stash the codes in a lookup table
  private static final Map<Integer, ResponseCode> responseCodes = new HashMap<>();

  static {
    for (ResponseCode code : ResponseCode.values()) {
      if (responseCodes.get(code.getCodeValue()) != null) {
        GNSConfig.getLogger().log(Level.INFO, "DUPLICATE RESPONSE CODE {0} : {1}",
                new Object[]{code.name(), code.getCodeValue()});
      }
      responseCodes.put(code.getCodeValue(), code);
    }
  }


  public static ResponseCode getResponseCode(int codeValue) {
    return responseCodes.get(codeValue);
  }

  //
  private final int codeValue;
  private final String protocolCode;
  private final ResponseCodeType type;
  private String message = null;


  public static enum ResponseCodeType {

    NORMAL,

    EXCEPTION,

    ERROR;
  };

  private ResponseCode(int codeValue, String protocolCode, ResponseCodeType type) {
    this.codeValue = codeValue;
    this.protocolCode = protocolCode;
    this.type = type;
  }


  public ResponseCode setMessage(String msg) {
    this.message = msg;
    return this;
  }


  public String getMessage() {
    return this.message;
  }


  public int getCodeValue() {
    return codeValue;
  }


  public String getProtocolCode() {
    return protocolCode;
  }


  public boolean isExceptionOrError() {
    return type == ResponseCodeType.ERROR || type == ResponseCodeType.EXCEPTION;
  }


  public boolean isOKResult() {
    return !isExceptionOrError();
  }


  public boolean isError() {
    return type == ResponseCodeType.ERROR;
  }


  public boolean isException() {
    return type == ResponseCodeType.EXCEPTION;
  }

  private static String generateSwiftConstants() {
    StringBuilder result = new StringBuilder();
    for (ResponseCode entry : ResponseCode.values()) {
      result.append("    public static let ");
      result.append(entry.name().toUpperCase());
      result.append("\t\t\t\t = ");
      result.append(entry.getCodeValue());
      result.append("\n");
    }
    return result.toString();
  }


public static void main(String args[]) {
    System.out.println(generateSwiftConstants());
  }
}
