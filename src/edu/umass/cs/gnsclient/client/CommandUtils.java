/* Copyright (1c) 2016 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (1the "License"); you may not
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
 * Initial developer(s): Westy */
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gnsclient.client.GNSClientConfig.GNSCC;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import edu.umass.cs.gnscommon.exceptions.client.AclException;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.DuplicateNameException;
import edu.umass.cs.gnscommon.exceptions.client.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.client.InvalidFieldException;
import edu.umass.cs.gnscommon.exceptions.client.InvalidGuidException;
import edu.umass.cs.gnscommon.exceptions.client.VerificationException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.packets.ResponsePacket;
import edu.umass.cs.gnscommon.utils.CanonicalJSON;
import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnscommon.exceptions.client.OperationNotSupportedException;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;

import java.util.Date;
import java.util.Random;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import edu.umass.cs.gnscommon.GNSProtocol;

/**
 *
 * @author arun, westy
 */
public class CommandUtils {


  private static Random random;
  static {
    random = new Random();
  }
  /**
   * @return Random long.
   */
  public static String getRandomRequestNonce() {
    return (random.nextLong() + "");
  }

  /**
   * Creates a command object from the given action string and a variable
   * number of key and value pairs.
   *
   * @param commandType
   *
   * @param keysAndValues
   * @return the query string
   * @throws JSONException
   */
  public static JSONObject createCommand(CommandType commandType,
          Object... keysAndValues) throws JSONException {
    long startTime = System.currentTimeMillis();
    JSONObject result = new JSONObject();
    String key;
    Object value;
    result.put(GNSProtocol.COMMAND_INT.toString(), commandType.getInt());
    for (int i = 0; i < keysAndValues.length; i = i + 2) {
      key = (String) keysAndValues[i];
      value = keysAndValues[i + 1];
      result.put(key, value);
    }

    DelayProfiler.updateDelay("createCommand", startTime);
    return result;
  }

  /**
   * Only for backwards compatibility
   *
   * @param response
   * @return Single value string if response is JSONObject with a single
   * key-value piar.
   */
  public static String specialCaseSingleField(String response) {
    if (JSONPacket.couldBeJSON(response) && response.startsWith("{")) {
      try {
        JSONObject json = new JSONObject(response);
        String[] keys = CanonicalJSON.getNames(json);
        return (keys.length == 1) ? json.getString(CanonicalJSON
                .getNames(json)[0]) : response;
      } catch (JSONException e) {
        e.printStackTrace();
      }
    }
    return response;
  }

  /**
   * Conditionally converts a returned string to a JSON Array.
   *
   * @param field
   * @param response
   * @return a JSON Array
   * @throws JSONException
   */
  public static JSONArray commandResponseToJSONArray(String field, String response) throws JSONException {
    if (JSONPacket.couldBeJSONArray(response)) {
      return new JSONArray(response);
    } else {
      return new JSONObject(response).getJSONArray(field);
    }
  }



  /**
   * This little dance is because we need to remove the signature to get the
   * message that was signed. Alternatively we could have the client do it but
   * that just means a longer message OR we could put the signature outside
   * the command in the packet but some packets don't need a signature
   *
   * @param command
   * @throws JSONException
   */
  public static void addMessageWithoutSignatureToJSON(JSONObject command) throws JSONException {
    if (command.has(GNSProtocol.SIGNATURE.toString())) {
      String signature = command.getString(GNSProtocol.SIGNATURE.toString());
      command.remove(GNSProtocol.SIGNATURE.toString());
      String commandSansSignature = CanonicalJSON.getCanonicalForm(command);
      command.put(GNSProtocol.SIGNATURE.toString(), signature).put(GNSProtocol.SIGNATUREFULLMESSAGE.toString(),
              commandSansSignature);
    }
  }

  /**
   * @param cvrp
   * @return Response
   * @throws ClientException
   */
  protected static ResponsePacket checkResponseOldSchool(ResponsePacket cvrp) throws ClientException {
    checkResponseOldSchool(cvrp.getReturnValue());
    return cvrp;
  }

  /**
   * Checks the response from a command request for proper syntax as well as
   * converting error responses into the appropriate thrown GNS exceptions.
   * This method is only used by the HTTP server.
   *
   * In the original protocol the string response was modeled after other
   * simple string-based response protocols. Responses were either:<br>
   * 1) a return value whose format was interpreted by the caller - this is
   * the nominal case<br>
   * 2) "+OK+" - which was used to indicate a nominal result for commands that
   * don't return a value<br>
   * 2) "+NULL+" - another nominal which meant we should return null as the
   * value<br>
   * 3) "+NO+"{space}{error code string}{{space}{additional info string}}+<br>
   * Later a special case 4 was added for GNSProtocol.ACTIVE_REPLICA_EXCEPTION.toString().<br>
   *
   * For case 3 the additional info strings (could be any number) were
   * interpreted by the error handlers and generally used to help provide
   * additional info to indicate error causes.
   *
   * Also note that:<br>
   * GNSCommandProtocol.OK_RESPONSE = "+OK+"<br>
   * GNSProtocol.BAD_RESPONSE.toString() = "+NO+"<br>
   * GNSProtocol.NULL_RESPONSE.toString() = "+NULL+"<br>
   *
   * @param response
   * @return Response as string.
   * @throws ClientException
   */
  public static String checkResponseOldSchool(String response) throws ClientException {
    // System.out.println("response:" + response);
    if (response.startsWith(GNSProtocol.BAD_RESPONSE.toString())) {
      String[] results = response.split(" ");
      // System.out.println("results length:" + results.length);
      if (results.length < 2) {
        throw new ClientException("Invalid bad response indicator: "
                + response);
      } else if (results.length >= 2) {
        // System.out.println("results[0]:" + results[0]);
        // System.out.println("results[1]:" + results[1]);
        String error = results[1];
        // deal with the rest
        StringBuilder parts = new StringBuilder();
        for (int i = 2; i < results.length; i++) {
          parts.append(" ");
          parts.append(results[i]);
        }
        String rest = parts.toString();
        if (error.startsWith(GNSProtocol.BAD_SIGNATURE.toString())) {
          throw new EncryptionException(error);
        }
        if (error.startsWith(GNSProtocol.BAD_GUID.toString())
                || error.startsWith(GNSProtocol.BAD_ACCESSOR_GUID.toString())
                // why not with GNSProtocol.DUPLICATE_NAME.toString()?
                || error.startsWith(GNSProtocol.DUPLICATE_GUID.toString())
                || error.startsWith(GNSProtocol.BAD_ACCOUNT.toString())) {
          throw new InvalidGuidException(error + rest);
        }
        if (error.startsWith(GNSProtocol.DUPLICATE_FIELD.toString())) {
          throw new InvalidFieldException(error + rest);
        }
        if (error.startsWith(GNSProtocol.FIELD_NOT_FOUND.toString())) {
          throw new FieldNotFoundException(error + rest);
        }
        if (error.startsWith(GNSProtocol.ACCESS_DENIED.toString())) {
          throw new AclException(error + rest);
        }
        if (error.startsWith(GNSProtocol.DUPLICATE_NAME.toString())) {
          throw new DuplicateNameException(error + rest);
        }
        if (error.startsWith(GNSProtocol.VERIFICATION_ERROR.toString())) {
          throw new VerificationException(error + rest);
        }
        if (error
                .startsWith(GNSProtocol.ALREADY_VERIFIED_EXCEPTION.toString())) {
          throw new VerificationException(error + rest);
        }
        if (error.startsWith(GNSProtocol.OPERATION_NOT_SUPPORTED.toString())) {
          throw new OperationNotSupportedException(error + rest);
        }
        throw new ClientException("General command failure: " + error
                + rest);
      }
    }
    if (response.startsWith(GNSProtocol.NULL_RESPONSE.toString())) {
      return null;
    } else if (response
            .startsWith(GNSProtocol.ACTIVE_REPLICA_EXCEPTION.toString()
                    .toString())) {
      throw new InvalidGuidException(response);
    } else {
      return response;
    }
  }

  /**
   *
   * @param command
   *
   * @param responsePacket
   * @return Response as a string.
   * @throws ClientException
   */
  public static ResponsePacket checkResponse(
          ResponsePacket responsePacket, CommandPacket command) throws ClientException {

    ResponseCode code = responsePacket.getErrorCode();
    String returnValue = responsePacket.getReturnValue();
    // If the code isn't an error or exception we're just returning the
    // return value. Also handle the special case where the command
    // wants to return a null value.
    if (code.isOKResult()) {
      return (returnValue.startsWith(GNSProtocol.NULL_RESPONSE.toString())) ? null
              : responsePacket;//returnValue;
    }
    // else error
    String errorSummary = code
            + ": "
            + returnValue
            //+ ": " + responsePacket.getSummary()
            + (command != null ? " for command " + command.getSummary()
                    : "");
    switch (code) {
      case SIGNATURE_ERROR:
        throw new EncryptionException(code, errorSummary);

      case BAD_GUID_ERROR:
      case BAD_ACCESSOR_ERROR:
      case BAD_ACCOUNT_ERROR:
        throw new InvalidGuidException(code, errorSummary);

      case FIELD_NOT_FOUND_ERROR:
        throw new FieldNotFoundException(code, errorSummary);
      case ACCESS_ERROR:
        throw new AclException(code, errorSummary);
      case VERIFICATION_ERROR:
        throw new VerificationException(code, errorSummary);
      case ALREADY_VERIFIED_EXCEPTION:
        throw new VerificationException(code, errorSummary);
      case DUPLICATE_ID_EXCEPTION:
      //case DUPLICATE_GUID_EXCEPTION:
      //case DUPLICATE_NAME_EXCEPTION:
        throw new DuplicateNameException(code, errorSummary);
      case DUPLICATE_FIELD_EXCEPTION:
        throw new InvalidFieldException(code, errorSummary);

      case ACTIVE_REPLICA_EXCEPTION:
        throw new InvalidGuidException(code, errorSummary);
      case NONEXISTENT_NAME_EXCEPTION:
        throw new InvalidGuidException(code, errorSummary);

      case TIMEOUT:
      case RECONFIGURATION_EXCEPTION:
          throw new ClientException(code, errorSummary);    	  

      default:
        throw new ClientException(code,
                "Error received with an unknown response code: "
                + errorSummary);
    }
  }

  /**
   * Creates a JSON Object from the given command, keypair and a variable
   * number of key and value pairs. Includes a NONCE and TIMESTAMP field.
   * 
   * @param commandType
   * @param includeTimestamp
   * @param keysAndValues
   * @return a JSONObject
   * @throws ClientException
   * @throws JSONException
   */
  public static JSONObject createCommandWithTimestampAndNonce(CommandType commandType, boolean includeTimestamp,
          Object... keysAndValues)
          throws ClientException, JSONException {
    JSONObject result = createCommand(commandType, keysAndValues);
    if (includeTimestamp) {
      result.put(GNSProtocol.TIMESTAMP.toString(),
              Format.formatDateISO8601UTC(new Date()));
    }
    result.put(GNSProtocol.NONCE.toString(), getRandomRequestNonce());
    return result;
  }
  
  /**
   * Creates a JSON Object from the given command, keypair and a variable
   * number of key and value pairs.
   * 
   * @param commandType
   * @param guidEntry
   * @param keysAndValues
   * @return Signed command.
   * @throws ClientException
   */
  public static JSONObject createAndSignCommandInternal(CommandType commandType,
          GuidEntry guidEntry, Object... keysAndValues)
          throws ClientException {
    try {
      JSONObject result = createCommandWithTimestampAndNonce(commandType, true, keysAndValues);
      String canonicalJSON = CanonicalJSON.getCanonicalForm(result);
      String signatureString = null;
      long t = System.nanoTime();
      if (Config.getGlobalBoolean(GNSCC.ENABLE_SECRET_KEY)) {
        signatureString = CryptoUtils.signDigestOfMessageSecretKey(guidEntry, canonicalJSON);
      } else {
        signatureString = CryptoUtils.signDigestOfMessage(guidEntry, canonicalJSON);
      }
      result.put(GNSProtocol.SIGNATURE.toString(), signatureString);
      if (edu.umass.cs.utils.Util.oneIn(10)) {
        DelayProfiler.updateDelayNano("signature", t);
      }
      return result;
    } catch (JSONException e) {
      throw new ClientException("Error encoding message", e);
    }
  }

  /**
   * @param commandType
   * @param querier
   * @param keysAndValues
   * @return JSONObject command
   * @throws ClientException
   */
  public static JSONObject createAndSignCommand(CommandType commandType,
          GuidEntry querier, Object... keysAndValues) throws ClientException {
    try {
      return querier != null ? createAndSignCommandInternal(commandType,
              querier, keysAndValues)
              : createCommand(commandType, keysAndValues);
    } catch (JSONException e) {
      throw new ClientException("Error encoding message", e);
    }
  }
}
