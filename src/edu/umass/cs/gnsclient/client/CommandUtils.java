
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.AclException;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.DuplicateNameException;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import edu.umass.cs.gnscommon.exceptions.client.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.client.InvalidFieldException;
import edu.umass.cs.gnscommon.exceptions.client.InvalidGuidException;
import edu.umass.cs.gnscommon.exceptions.client.OperationNotSupportedException;
import edu.umass.cs.gnscommon.exceptions.client.VerificationException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.packets.ResponsePacket;
import edu.umass.cs.gnscommon.utils.CanonicalJSON;
import edu.umass.cs.nio.JSONPacket;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;



public class CommandUtils {



  public static String specialCaseSingleField(String response) {
    if (JSONPacket.couldBeJSON(response) && response.startsWith("{")) {
      try {
        JSONObject json = new JSONObject(response);
        String[] keys = CanonicalJSON.getNames(json);
        return (keys.length == 1) ? json.getString(JSONObject
                .getNames(json)[0]) : response;
      } catch (JSONException e) {
        e.printStackTrace();
      }
    }
    return response;
  }


  public static JSONArray commandResponseToJSONArray(String field, String response) throws JSONException {
    if (JSONPacket.couldBeJSONArray(response)) {
      return new JSONArray(response);
    } else {
      return new JSONObject(response).getJSONArray(field);
    }
  }



  public static void addMessageWithoutSignatureToJSON(JSONObject command) throws JSONException {
    if (command.has(GNSProtocol.SIGNATURE.toString())) {
      String signature = command.getString(GNSProtocol.SIGNATURE.toString());
      command.remove(GNSProtocol.SIGNATURE.toString());
      String commandSansSignature = CanonicalJSON.getCanonicalForm(command);
      command.put(GNSProtocol.SIGNATURE.toString(), signature).put(GNSProtocol.SIGNATUREFULLMESSAGE.toString(),
              commandSansSignature);
    }
  }


  protected static ResponsePacket checkResponseOldSchool(ResponsePacket cvrp) throws ClientException {
    checkResponseOldSchool(cvrp.getReturnValue());
    return cvrp;
  }


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


}
