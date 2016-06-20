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

import edu.umass.cs.gnsserver.gnsapp.QueryResult;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * This class describes the error and exception codes for GNS packets that get
 * sent back to the server and the client.
 *
 * These codes are used by the GNS to communicate via response packets why a
 * value could not be returned from the server(s) to the client. * This class is
 * a sibling to {@link QueryResult} which is used when we're returning a value
 * and not just whether the request was successful.
 *
 * @author arun, westy
 *
 */
public enum GNSResponseCode implements Serializable {
  /**
   * A positive acknowledgment. This indicates that the
   * return value contains the result of the command or in
   * the case of commands that don't return a value that the
   * command was executed without error.
   * For other response codes the return value only contains
   * additional information to describe the error or exception.
   *//**
   * A positive acknowledgment. This indicates that the
   * return value contains the result of the command or in
   * the case of commands that don't return a value that the
   * command was executed without error.
   * For other response codes the return value only contains
   * additional information to describe the error or exception.
   */
  NO_ERROR(200, GNSCommandProtocol.OK_RESPONSE, TYPE.NORMAL),
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
  // The following are access or signature errors
  /**
   * Bad signature. An access or signature error.
   */
  SIGNATURE_ERROR(5, GNSCommandProtocol.BAD_SIGNATURE, TYPE.ERROR),
  /**
   * Stale signature or key.
   */
  STALE_COMMAND_VALUE(6, GNSCommandProtocol.STALE_COMMMAND, TYPE.ERROR),
  /**
   * Access denied.
   */
  ACCESS_ERROR(7, GNSCommandProtocol.ACCESS_DENIED, TYPE.ERROR),
  /**
   * Non-existent GUID.
   */
  BAD_GUID_ERROR(8, GNSCommandProtocol.BAD_GUID, TYPE.ERROR),
  /**
   * Non-existent accessor GUID.
   */
  BAD_ACCESSOR_ERROR(9, GNSCommandProtocol.BAD_ACCESSOR_GUID, TYPE.ERROR),
  /**
   * An error during account GUID verification.
   */
  VERIFICATION_ERROR(10, GNSCommandProtocol.VERIFICATION_ERROR, TYPE.ERROR),
  /**
   * Account does not exist.
   */
  BAD_ACCOUNT_ERROR(11, GNSCommandProtocol.BAD_ACCOUNT, TYPE.ERROR),
  /**
   *
   */
  OPERATION_NOT_SUPPORTED(404, GNSCommandProtocol.OPERATION_NOT_SUPPORTED,
          TYPE.ERROR),
  /* Errors above, exceptions below. The distinction is that the former is
	 * more serious and irrecoverable for that operation, but the latter may
	 * sometimes happen in the otherwise normal course of events. */
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
   *
   */
  QUERY_PROCESSING_ERROR(405, GNSCommandProtocol.QUERY_PROCESSING_ERROR,
          TYPE.ERROR),
  /**
   *
   */
  NO_ACTION_FOUND(407, GNSCommandProtocol.NO_ACTION_FOUND, TYPE.EXCEPTION),
  /**
   *
   */
  TIMEOUT(408, GNSCommandProtocol.TIMEOUT, TYPE.EXCEPTION),;

  // stash the codes in a lookup table
  private static final Map<Integer, GNSResponseCode> responseCodes = new HashMap<Integer, GNSResponseCode>();

  static {
    for (GNSResponseCode code : GNSResponseCode.values()) {
      assert (responseCodes.put(code.getCodeValue(), code) == null);
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
  public static GNSResponseCode getResponseCode(int codeValue) {
    return responseCodes.get(codeValue);
  }

  //
  private final int codeValue;
  private final String protocolCode;
  private final TYPE type;

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

  private GNSResponseCode(int codeValue, String protocolCode, TYPE type) {
    this.codeValue = codeValue;
    this.protocolCode = protocolCode;
    this.type = type;
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
