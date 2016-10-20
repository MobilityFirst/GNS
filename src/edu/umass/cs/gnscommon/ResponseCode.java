/* Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): Westy 
 */
package edu.umass.cs.gnscommon;

import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

/**
 * This class describes the error and exception codes for GNS packets that get
 * sent back to the server and the client.
 *
 * These codes are used by the GNS to communicate via response packets why a
 * value could not be returned from the server(s) to the client.
 *
 * @author arun, westy
 *
 */
public enum ResponseCode implements Serializable {
  /**
   * A positive acknowledgment. This indicates that the
   * return value contains the result of the command or in
   * the case of commands that don't return a value that the
   * command was executed without error.
   * For other response codes the return value only contains
   * additional information to describe the error or exception.
   */
  NO_ERROR(200, GNSProtocol.OK_RESPONSE.toString(), TYPE.NORMAL),
  /**
   * Unspecified error. This should be used replaced with more specific errors
   * in most cases and used sparingly, if at all because it doesn't convey
   * enough information.
   */
  UNSPECIFIED_ERROR(1, GNSCommandProtocol.UNSPECIFIED_ERROR, TYPE.ERROR),
  /**
   * Field in a record was not found.
   */
  FIELD_NOT_FOUND_ERROR(2, GNSCommandProtocol.FIELD_NOT_FOUND, TYPE.ERROR),
  // The following three are access or signature errors
  /**
   * Bad signature. An access or signature error.
   * This will happen when the message in a command packet fails signature verification.
   * See {@link edu.umass.cs.gnsserver.gnsapp.clientSupport.NSAuthentication#signatureAndACLCheck}
   */
  SIGNATURE_ERROR(5, GNSCommandProtocol.BAD_SIGNATURE, TYPE.ERROR),
  /**
   * Stale signature or key. An access or signature error.
   * This will happen when a command packet arrives that is too old.
   *
   * See {@link edu.umass.cs.gnsserver.gnsapp.clientSupport.NSAuthentication#signatureAndACLCheck}
   */
  STALE_COMMAND_VALUE(6, GNSCommandProtocol.STALE_COMMMAND, TYPE.ERROR),
  /**
   * Access denied. An access or signature error.
   * This will happen when a command packet arrives that tries to access a field
   * for which it does not have the correct access control.
   *
   * See {@link edu.umass.cs.gnsserver.gnsapp.clientSupport.NSAuthentication#signatureAndACLCheck}
   */
  ACCESS_ERROR(7, GNSCommandProtocol.ACCESS_DENIED, TYPE.ERROR),
  /**
   * Non-existent GUID.
   * This will happen when a command packet arrives that tries to access a guid
   * that does not exist.
   */
  BAD_GUID_ERROR(8, GNSCommandProtocol.BAD_GUID, TYPE.ERROR),
  /**
   * Non-existent accessor GUID.
   * This will happen when a command packet arrives that is trying to access
   * a field using an accessor guid that does not exist.
   */
  BAD_ACCESSOR_ERROR(9, GNSCommandProtocol.BAD_ACCESSOR_GUID, TYPE.ERROR),
  /**
   * An error during account GUID verification.
   * This will happen if the verification code is incorrect.
   */
  VERIFICATION_ERROR(10, GNSCommandProtocol.VERIFICATION_ERROR, TYPE.ERROR),
  /**
   * Account guid does not exist.
   *
   */
  BAD_ACCOUNT_ERROR(11, GNSCommandProtocol.BAD_ACCOUNT, TYPE.ERROR),
  /**
   * The operation does not exist or does not except the arguments given.
   */
  OPERATION_NOT_SUPPORTED(404, GNSCommandProtocol.OPERATION_NOT_SUPPORTED,
          TYPE.ERROR),
  /* Errors above, exceptions below. The distinction is that the former is
	 * more serious and irrecoverable for that operation, but the latter may
	 * sometimes happen in the otherwise normal course of events. */
  /**
   * Account has already been verified.
   */
  ALREADY_VERIFIED_EXCEPTION(12, GNSCommandProtocol.ALREADY_VERIFIED_EXCEPTION, TYPE.EXCEPTION),
  /**
   * Duplicate GUID or HRN.
   */
  DUPLICATE_ID_EXCEPTION(14,
          ClientReconfigurationPacket.ResponseCodes.DUPLICATE_ERROR
          .toString(), TYPE.EXCEPTION),
  /**
   * Duplicate field in a record.
   */
  DUPLICATE_FIELD_EXCEPTION(15, GNSCommandProtocol.DUPLICATE_FIELD,
          TYPE.EXCEPTION),
  /**
   * The name for which a RequestActiveReplicas was sent was found to not
   * exist at a reconfigurator. This can happen simply because a laggard
   * reconfigurator is catching up. If a CreateServiceName confirmation has
   * actually been received, it probably means that retrying the application
   * request (that will internally retry the RequestActiveReplicas request)
   * will probably succeed if a sufficient number of active replicas are
   * alive.
   *
   * The default reconfiguration client now automatically retries
   * RequestActiveReplicas requests before giving up, so this exception is
   * extremely unlikely under failure-free execution if the name actually
   * exists, i.e., if a CreateServiceName confirmation was previously received
   * and the name has not subsequently been deleted.
   */
  NONEXISTENT_NAME_EXCEPTION(16,
          ClientReconfigurationPacket.ResponseCodes.NONEXISTENT_NAME_ERROR
          .toString(), TYPE.EXCEPTION),
  /**
   * When an application request is received by an active replica where no
   * replica for the name exists. This does not necessarily mean that the name
   * itself does not exist, e.g., the name may have successfully gotten
   * created but one or a minority of laggard replicas may not yet have
   * created a Paxos instance for the name (but will do so eventually unless
   * the name is deleted earlier).
   */
  ACTIVE_REPLICA_EXCEPTION(17,
          ClientReconfigurationPacket.ResponseCodes.ACTIVE_REPLICA_EXCEPTION
          .toString(), TYPE.EXCEPTION),
  /**
   * The alias does not exist.
   */
  BAD_ALIAS_EXCEPTION(18, GNSCommandProtocol.BAD_ALIAS, TYPE.EXCEPTION),
  /**
   * The ACL type does not exist.
   */
  BAD_ACL_TYPE_ERROR(19, GNSCommandProtocol.BAD_ACL_TYPE, TYPE.ERROR),
  /**
   * The field does not exist.
   */
  FIELD_NOT_FOUND_EXCEPTION(20, GNSCommandProtocol.FIELD_NOT_FOUND, TYPE.EXCEPTION),
  /**
   * The guid is a duplicate of an already existing guid.
   */
  DUPLICATE_GUID_EXCEPTION(21, GNSCommandProtocol.DUPLICATE_GUID, TYPE.EXCEPTION),
  /**
   * The HRN already exists.
   */
  DUPLICATE_NAME_EXCEPTION(22, GNSCommandProtocol.DUPLICATE_NAME, TYPE.EXCEPTION),
  /**
   * A JSON parsing error occurred.
   */
  JSON_PARSE_ERROR(23, GNSCommandProtocol.JSON_PARSE_ERROR, TYPE.ERROR),
  /**
   * The max alias limit was exceeded.
   */
  TOO_MANY_ALIASES_EXCEPTION(24, GNSCommandProtocol.TOO_MANY_ALIASES, TYPE.EXCEPTION),
  /**
   * The max guid limit was exceeded.
   */
  TOO_MANY_GUIDS_EXCEPTION(25, GNSCommandProtocol.TOO_MANY_GUIDS, TYPE.EXCEPTION),
  /**
   * There was an error while updating a record.
   */
  UPDATE_ERROR(26, GNSCommandProtocol.UPDATE_ERROR, TYPE.ERROR),
  /**
   * Something went wrong while we were reading from or writing to the database.
   */
  DATABASE_OPERATION_ERROR(27, GNSCommandProtocol.DATABASE_OPERATION_ERROR, TYPE.ERROR),
  /**
   * An error occurred during the processing of a command query.
   */
  QUERY_PROCESSING_ERROR(405, GNSCommandProtocol.QUERY_PROCESSING_ERROR, TYPE.ERROR),
  /**
   * A timeout occurred.
   */
  TIMEOUT(408, GNSCommandProtocol.TIMEOUT, TYPE.EXCEPTION),
  /**
   * A remote query failed on the server side.
   */
  REMOTE_QUERY_EXCEPTION(410, GNSCommandProtocol.REMOTE_QUERY_EXCEPTION, TYPE.EXCEPTION),
  
	/**
	 * An internal request, possibly an active request, failed probably because the TTL expired
	 * or it was attempting to cause a cycle.
	 */
	INTERNAL_REQUEST_EXCEPTION(411, GNSProtocol.INTERNAL_REQUEST_EXCEPTION
			.toString(), TYPE.EXCEPTION),
  
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

  /**
   * Converts the integer response code value into a ResponseCode object. The
   * integer response codes are used to encode the response code when there
   * are sent in JSON encoded packets.
   *
   * @param codeValue
   * @return the NSResponseCode
   */
  public static ResponseCode getResponseCode(int codeValue) {
    return responseCodes.get(codeValue);
  }

  //
  private final int codeValue;
  private final String protocolCode;
  private final TYPE type;
  private String message = null;

  /**
   * The response code category.
   */
  public static enum TYPE {
    /**
     * No error or exceptional condition.
     */
    NORMAL,
    /**
     * Exceptional condition that is usually recoverable.
     */
    EXCEPTION,
    /**
     * Errors that are not usually recoverable.
     */
    ERROR;
  };

  private ResponseCode(int codeValue, String protocolCode, TYPE type) {
    this.codeValue = codeValue;
    this.protocolCode = protocolCode;
    this.type = type;
  }

  /**
   * Used to attach a message with the code when we want to return a code
   * rather than throw an exception; the code and message are expected to be
   * used upstream for possibly throwing an exception.
   *
   * @param msg
   * @return this
   */
  public ResponseCode setMessage(String msg) {
    this.message = msg;
    return this;
  }

  /**
   * @return Message attached to this code.
   */
  public String getMessage() {
    return this.message;
  }

  /**
   * Returns the integer equivalent of the code.
   *
   * @return an int
   */
  public int getCodeValue() {
    return codeValue;
  }

  /**
   * Returns the string that corresponds to this error in the client protocol.
   *
   * @return a string
   */
  public String getProtocolCode() {
    return protocolCode;
  }

  /**
   * Is this an exception or error code. Some aren't, some are.
   *
   * @return true if this is an error
   */
  public boolean isExceptionOrError() {
    return type == TYPE.ERROR || type == TYPE.EXCEPTION;
  }

  /**
   * Is this NOT an exception or error code.
   * Convenience method. See {@link #isExceptionOrError()}.
   *
   * @return True if no exception or error.
   */
  public boolean isOKResult() {
    return !isExceptionOrError();
  }

  /**
   * Is this an error code. Some aren't, some are.
   *
   * @return true if this is an error
   */
  public boolean isError() {
    return type == TYPE.ERROR;
  }

  /**
   * @return True if this is an exception.
   */
  public boolean isException() {
    return type == TYPE.EXCEPTION;
  }
}
