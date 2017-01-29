
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnscommon.exceptions.server.ServerRuntimeException;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnsserver.utils.Email;
import edu.umass.cs.gnsserver.gnsapp.GNSCommandInternal;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSFieldAccess;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DeleteServiceName;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;

import java.io.IOException;
import java.net.UnknownHostException;

import org.json.JSONException;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import javax.xml.bind.DatatypeConverter;
import org.json.JSONArray;
import org.json.JSONObject;


public class AccountAccess {


  public static final String ACCOUNT_INFO = InternalField
          .makeInternalFieldString("account_info");

  public static final String ACCOUNT_INFO_GUIDS = InternalField
          .makeInternalFieldString("guids");


  public static final String HRN_GUID = InternalField
          .makeInternalFieldString("guid");


  public static final String PRIMARY_GUID = InternalField
          .makeInternalFieldString("primary_guid");


  public static final String GUID_INFO = InternalField
          .makeInternalFieldString("guid_info");


  public static final String HRN_FIELD = InternalField
          .makeInternalFieldString("guid_info") + "." + GNSProtocol.NAME.toString();


  public static AccountInfo lookupAccountInfoFromGuidLocally(InternalRequestHeader header, String guid,
          ClientRequestHandlerInterface handler) {
    return lookupAccountInfoFromGuid(header, guid, handler, false);
  }


  public static AccountInfo lookupAccountInfoFromGuidAnywhere(InternalRequestHeader header, String guid,
          ClientRequestHandlerInterface handler) {
    return lookupAccountInfoFromGuid(header, guid, handler, true);
  }


  private static AccountInfo lookupAccountInfoFromGuid(InternalRequestHeader header, String guid,
          ClientRequestHandlerInterface handler, boolean allowRemoteLookup) {
    try {
      ValuesMap result = NSFieldAccess.lookupJSONFieldLocalNoAuth(null,
              guid, ACCOUNT_INFO, handler.getApp(), false);
      GNSConfig.getLogger().log(
              Level.FINE,
              "ValuesMap for {0} / {1}: {2}",
              new Object[]{guid, ACCOUNT_INFO,
                result != null ? result.getSummary() : result});
      if (result != null) {
        return new AccountInfo(new JSONObject(
                result.getString(ACCOUNT_INFO)));
      }
    } catch (FailedDBOperationException | JSONException | ParseException e) {
      // Do nothing as this is a normal result when the record doesn't
      // exist.
    }

    GNSConfig.getLogger().log(Level.FINE, "ACCOUNT_INFO NOT FOUND for {0}",
            guid);

    if (allowRemoteLookup) {
      GNSConfig.getLogger().log(Level.FINE,
              "LOOKING REMOTELY for ACCOUNT_INFO for {0}", guid);
      String value = null;
      try {
        value = handler.getInternalClient().execute(GNSCommandInternal.fieldRead(guid, ACCOUNT_INFO, header)).getResultString();
      } catch (IOException | JSONException | ClientException e) {
        // Do nothing as this is a normal result when the record doesn't
        // exist.
      } catch (InternalRequestException e) {
        e.printStackTrace();
      }
      if (value != null) {
        try {
          return new AccountInfo(new JSONObject(value));
        } catch (JSONException | ParseException e) {
          // Do nothing as this is a normal result when the record
          // doesn't exist.
        }
      }
    }
    return null;
  }


  public static String lookupPrimaryGuid(InternalRequestHeader header, String guid,
          ClientRequestHandlerInterface handler, boolean allowRemoteLookup) {
    try {
      ValuesMap result = NSFieldAccess.lookupJSONFieldLocalNoAuth(null,
              guid, PRIMARY_GUID, handler.getApp(), false);
      GNSConfig.getLogger().log(Level.FINE,
              "ValuesMap for {0} / {1}: {2}",
              new Object[]{guid, PRIMARY_GUID, result});
      if (result != null) {
        return result.getString(PRIMARY_GUID);
      }
    } catch (FailedDBOperationException | JSONException e) {
      GNSConfig.getLogger().log(Level.SEVERE,
              "Problem extracting PRIMARY_GUID from {0} :{1}",
              new Object[]{guid, e});
    }
    String value = null;
    GNSConfig.getLogger().log(Level.FINE,
            "PRIMARY_GUID NOT FOUND LOCALLY for {0}", guid);

    if (allowRemoteLookup) {
      GNSConfig.getLogger().log(Level.FINE,
              "LOOKING REMOTELY for PRIMARY_GUID for {0}", guid);
      try {
        value = handler.getInternalClient().execute(GNSCommandInternal.fieldRead(guid, PRIMARY_GUID, header)).getResultString();
        if (!FieldAccess.SINGLE_FIELD_VALUE_ONLY && value != null) {
          value = new JSONObject(value).getString(PRIMARY_GUID);
        }
      } catch (IOException | JSONException | ClientException | InternalRequestException e) {
        GNSConfig
                .getLogger()
                .log(Level.SEVERE,
                        "Problem getting HRN_GUID for {0} from remote server: {1}",
                        new Object[]{guid, e});
      }
    }
    return value;
  }


  public static String lookupGuidAnywhere(InternalRequestHeader header, String name,
          ClientRequestHandlerInterface handler) {
    return lookupGuid(header, name, handler, true);
  }


  public static String lookupGuidLocally(InternalRequestHeader header, String name,
          ClientRequestHandlerInterface handler) {
    return lookupGuid(header, name, handler, false);
  }


  private static String lookupGuid(InternalRequestHeader header, String name,
          ClientRequestHandlerInterface handler, boolean allowRemoteLookup) {
    try {
      ValuesMap result = NSFieldAccess.lookupJSONFieldLocalNoAuth(null,
              name, HRN_GUID, handler.getApp(), false);
      GNSConfig.getLogger().log(Level.FINE,
              "ValuesMap for {0} / {1}: {2}",
              new Object[]{name, HRN_GUID, result});
      if (result != null) {
        return result.getString(HRN_GUID);
      }
    } catch (FailedDBOperationException | JSONException e) {
      GNSConfig.getLogger().log(Level.SEVERE,
              "Problem extracting HRN_GUID from {0} :{1}",
              new Object[]{name, e});
    }
    //
    String value = null;
    GNSConfig.getLogger().log(Level.FINE, "HRN_GUID NOT FOUND for {0}",
            name);
    if (allowRemoteLookup) {
      GNSConfig.getLogger().log(Level.FINE,
              "LOOKING REMOTELY for HRN_GUID for {0}", name);
      try {
        value = handler.getInternalClient().
                execute(GNSCommandInternal.fieldRead(name, HRN_GUID, header)).getResultString();
        if (!FieldAccess.SINGLE_FIELD_VALUE_ONLY && value != null) {
          GNSConfig.getLogger().log(Level.FINE,
                  "Found HRN_GUID for {0}:{1}",
                  new Object[]{name, value});
          value = new JSONObject(value).getString(HRN_GUID);
        }
      } catch (IOException | JSONException | ClientException | InternalRequestException e) {
        GNSConfig
                .getLogger()
                .log(Level.SEVERE,
                        "Problem getting HRN_GUID for {0} from remote server: {1}",
                        new Object[]{name, e});
      }
    }
    return value;
  }


  public static GuidInfo lookupGuidInfoLocally(InternalRequestHeader header, String guid,
          ClientRequestHandlerInterface handler) {
    return lookupGuidInfo(header, guid, handler, false);
  }


  public static GuidInfo lookupGuidInfoAnywhere(InternalRequestHeader header, String guid,
          ClientRequestHandlerInterface handler) {
    return lookupGuidInfo(header, guid, handler, true);
  }


  private static GuidInfo lookupGuidInfo(InternalRequestHeader header, String guid,
          ClientRequestHandlerInterface handler, boolean allowRemoteLookup) {
    GNSConfig.getLogger().log(Level.FINE, "allowRemoteLookup is {0}",
            allowRemoteLookup);
    try {
      ValuesMap result = NSFieldAccess.lookupJSONFieldLocalNoAuth(null,
              guid, GUID_INFO, handler.getApp(), false);
      GNSConfig.getLogger().log(
              Level.FINE,
              "ValuesMap for {0} / {1} {2}",
              new Object[]{guid, GUID_INFO,
                result != null ? result.getSummary() : result});
      if (result != null) {
        return new GuidInfo(new JSONObject(result.getString(GUID_INFO)));
      }
    } catch (FailedDBOperationException | JSONException | ParseException e) {
      GNSConfig.getLogger().log(Level.SEVERE,
              "Problem extracting GUID_INFO from {0} :{1}",
              new Object[]{guid, e});
    }

    GNSConfig.getLogger().log(Level.FINE, "GUID_INFO NOT FOUND for {0}",
            guid);
    if (allowRemoteLookup) {
      GNSConfig.getLogger().log(Level.FINE,
              "LOOKING REMOTELY for GUID_INFO for {0}", guid);
      String value = null;
      Object obj = null;
      try {
        value = (obj = handler
                .getInternalClient()
                .execute(
                        GNSCommandInternal.fieldRead(guid, GUID_INFO,
                                header)).getResultMap().get(GUID_INFO)) != null ? obj
                .toString() : value;
      } catch (IOException | JSONException | ClientException | InternalRequestException e) {
        GNSConfig
                .getLogger()
                .log(Level.SEVERE,
                        "Problem getting GUID_INFO for {0} from remote server: {1}",
                        new Object[]{guid, e});
      }
      if (value != null) {
        try {
          return new GuidInfo(new JSONObject(value));
        } catch (JSONException | ParseException e) {
          GNSConfig
                  .getLogger()
                  .log(Level.SEVERE,
                          "Problem parsing GUID_INFO value from remote server for {0}: {1}",
                          new Object[]{guid, e});
        }
      }
    }
    return null;
  }


  public static AccountInfo lookupAccountInfoFromNameAnywhere(InternalRequestHeader header, String name,
          ClientRequestHandlerInterface handler) {
    String guid = lookupGuidAnywhere(header, name, handler);
    if (guid != null) {
      return lookupAccountInfoFromGuidAnywhere(header, guid, handler);
    }
    return null;
  }

  private static final String VERIFY_COMMAND = "account_verify";
  private static final String EMAIL_BODY = "Hi %2$s,\n\n"
          + "This is an automated message informing you that %1$s has created\n"
          + "an account for %2$s. You were sent this message to insure that the\n"
          + "person that created this account actually has access to this email address.\n\n"
          + "To verify this is your email address you can click on the link below.\n"
          + "If you are unable to click on the link, you can complete your email address\n"
          + "verification by copying and pasting the URL into your web browser:\n\n"
          + "http://%3$s/"
          + Config.getGlobalString(GNSConfig.GNSC.HTTP_SERVER_GNS_URL_PATH)
          + "/VerifyAccount?guid=%4$s&code=%5$s\n\n"
          + "If you previously verified your email address please do so again.\n"
          + "We apologize for the inconvenience. We needed to recreate your account information.\n\n"
          + "If you did not create this account you can just ignore this email and nothing bad will happen.\n\n"
          + "Thank you,\nThe CASA Team.";
  private static final String EMAIL_CLI_CONDITIONAL = "\n\nFor GNS CLI users only: enter this command into the CLI that you used to create the account:\n\n"
          + VERIFY_COMMAND + " %1$s %2$s\n\n";

  private static final String ADMIN_BODY = "This is an automated message informing "
          + "you that %1$s has created an account for %2$s on the GNS server at %3$s.\n"
          + "You can view their information using the link below:"
          + "\n\n%4$s%2$s\n";


  public static CommandResponse addAccount(InternalRequestHeader header,
          CommandPacket commandPacket,
          final String hostPortString, final String name, final String guid,
          String publicKey, String password, boolean useEmailVerification,
          ClientRequestHandlerInterface handler) throws ClientException,
          IOException, JSONException, InternalRequestException {

    CommandResponse response;
    // make this even if we don't need it
    String verifyCode = createVerificationCode(name);
    if ((response = addAccountInternal(header, name, guid, publicKey,
            password, useEmailVerification, verifyCode, handler))
            .getExceptionOrErrorCode().isOKResult()) {

      // Account creation was succesful so maybe send email verification.
      if (useEmailVerification) {
        boolean emailSent = sendEmailAuthentication(name, guid,
                hostPortString, verifyCode);
        if (emailSent) {
          return new CommandResponse(ResponseCode.NO_ERROR,
                  GNSProtocol.OK_RESPONSE.toString());
        } else {
          // if we can't send the confirmation back out of the account
          // creation
          AccountInfo accountInfo = lookupAccountInfoFromGuidAnywhere(header,
                  guid, handler);
          if (accountInfo != null) {
            removeAccount(header, commandPacket, accountInfo, handler);
          }
          return new CommandResponse(ResponseCode.VERIFICATION_ERROR,
                  GNSProtocol.BAD_RESPONSE.toString() + " "
                  + GNSProtocol.VERIFICATION_ERROR.toString()
                  + " " + "Unable to send verification email");
        }
      } else {
        GNSConfig.getLogger().fine(
                "**** EMAIL VERIFICATION IS OFF! ****");
      }
    }
    return response;
  }

  private static final int VERIFICATION_CODE_LENGTH = 3; // Six hex characters

  private static String createVerificationCode(String name) {
    // Don't really even need name here, but what the heck.
    return DatatypeConverter.printHexBinary(Arrays.copyOf(ShaOneHashFunction.getInstance()
            .hash(name + new String(Util.getRandomAlphanumericBytes(128))),
            VERIFICATION_CODE_LENGTH));
//    return ByteUtils.toHex(Arrays.copyOf(ShaOneHashFunction.getInstance()
//            .hash(name + new String(Util.getRandomAlphanumericBytes(128))),
//            VERIFICATION_CODE_LENGTH));
  }

  private static boolean sendEmailAuthentication(String name, String guid,
          String hostPortString, String verifyCode) {
    // Send out the confirmation email with a verification code
    String emailBody = String.format(EMAIL_BODY,
            Config.getGlobalString(GNSConfig.GNSC.APPLICATION_NAME), // 1$
            name, // 2$
            hostPortString, // 3$
            guid, // 4$
            verifyCode // 5$
    );
    if (Config.getGlobalBoolean(GNSConfig.GNSC.INCLUDE_CLI_NOTIFICATION)) {
      emailBody += String.format(EMAIL_CLI_CONDITIONAL, name, verifyCode);
    }
    String subject = Config
            .getGlobalString(GNSConfig.GNSC.APPLICATION_NAME)
            + " Account Authentication";
    boolean emailOK = Email.email(subject, name, emailBody);
    // do the admin email in another thread so it's faster and
    // because we don't care if it completes
    (new Thread() {
      @Override
      public void run() {
        Email.email(
                "GNS Account Notification",
                Config.getGlobalString(GNSConfig.GNSC.SUPPORT_EMAIL),
                String.format(
                        ADMIN_BODY,
                        Config.getGlobalString(GNSConfig.GNSC.APPLICATION_NAME), // 1$
                        name, // 2$
                        hostPortString, // 3$
                        Config.getGlobalString(GNSConfig.GNSC.STATUS_URL) // 4$
                ));
      }
    }).start();
    return emailOK;
  }


  public static CommandResponse resendAuthenticationEmail(
          InternalRequestHeader header, CommandPacket commandPacket,
          AccountInfo accountInfo, String guid, String signature,
          String message, ClientRequestHandlerInterface handler)
          throws UnknownHostException {
    if (Config.getGlobalBoolean(GNSConfig.GNSC.ENABLE_EMAIL_VERIFICATION)) {
      String name = accountInfo.getName();
      String code = createVerificationCode(name);
      boolean emailSent = sendEmailAuthentication(name, guid,
              handler.getHttpServerHostPortString(), code);
      if (emailSent) {
        accountInfo.setVerificationCode(code);
        accountInfo.noteUpdate();
        if (updateAccountInfoLocallyNoAuthentication(header, commandPacket, accountInfo,
                handler).isOKResult()) {
          return new CommandResponse(ResponseCode.NO_ERROR,
                  GNSProtocol.OK_RESPONSE.toString());
        } else {
          return new CommandResponse(ResponseCode.UPDATE_ERROR,
                  GNSProtocol.BAD_RESPONSE.toString() + " "
                  + GNSProtocol.UPDATE_ERROR.toString() + " "
                  + "Unable to update account info");
        }
      } else {
        return new CommandResponse(ResponseCode.VERIFICATION_ERROR,
                GNSProtocol.BAD_RESPONSE.toString() + " "
                + GNSProtocol.VERIFICATION_ERROR.toString()
                + " " + "Unable to send verification email");
      }
    }
    return new CommandResponse(ResponseCode.VERIFICATION_ERROR,
            GNSProtocol.BAD_RESPONSE.toString() + " "
            + GNSProtocol.VERIFICATION_ERROR.toString() + " "
            + "Email verification is disabled.");
  }


  public static CommandResponse verifyAccount(InternalRequestHeader header,
          CommandPacket commandPacket, String guid, String code,
          ClientRequestHandlerInterface handler) {
    GNSConfig.getLogger().log(Level.FINE,
            "*********** VERIFICATION CODE {0}", code);
    AccountInfo accountInfo;
    if ((accountInfo = lookupAccountInfoFromGuidLocally(header, guid, handler)) == null) {
      return new CommandResponse(ResponseCode.VERIFICATION_ERROR,
              GNSProtocol.BAD_RESPONSE.toString() + " "
              + GNSProtocol.VERIFICATION_ERROR.toString() + " "
              + "Unable to read account info");
    }
    if (accountInfo.isVerified()) {
      return new CommandResponse(ResponseCode.ALREADY_VERIFIED_EXCEPTION,
              GNSProtocol.BAD_RESPONSE.toString() + " "
              + GNSProtocol.ALREADY_VERIFIED_EXCEPTION.toString()
              + " Account already verified");
    }
    if (accountInfo.getVerificationCode() == null && code == null) {
      return new CommandResponse(ResponseCode.VERIFICATION_ERROR,
              GNSProtocol.BAD_RESPONSE.toString() + " "
              + GNSProtocol.VERIFICATION_ERROR.toString() + " "
              + "Bad verification code");
    }
    if (accountInfo.getCodeTime() == null) {
      return new CommandResponse(ResponseCode.VERIFICATION_ERROR,
              GNSProtocol.BAD_RESPONSE.toString() + " "
              + GNSProtocol.VERIFICATION_ERROR.toString() + " "
              + "Cannot retrieve account code time");
    }
    if ((new Date()).getTime() - accountInfo.getCodeTime().getTime() > Config
            .getGlobalInt(GNSConfig.GNSC.EMAIL_VERIFICATION_TIMEOUT_IN_HOURS) * 60 * 60 * 1000) {
      return new CommandResponse(ResponseCode.VERIFICATION_ERROR,
              GNSProtocol.BAD_RESPONSE.toString() + " "
              + GNSProtocol.VERIFICATION_ERROR.toString() + " "
              + "Account code no longer valid");
    }
    if (!accountInfo.getVerificationCode().equals(code)) {
      return new CommandResponse(ResponseCode.VERIFICATION_ERROR,
              GNSProtocol.BAD_RESPONSE.toString() + " "
              + GNSProtocol.VERIFICATION_ERROR.toString() + " "
              + "Code not correct");
    }
    accountInfo.setVerificationCode(null);
    accountInfo.setVerified(true);
    accountInfo.noteUpdate();
    if (updateAccountInfoLocallyNoAuthentication(header, commandPacket, accountInfo, handler).isOKResult()) {
      return new CommandResponse(ResponseCode.NO_ERROR,
              GNSProtocol.OK_RESPONSE.toString() + " "
              + "Your account has been verified."); // add a
    } else {
      return new CommandResponse(ResponseCode.UPDATE_ERROR,
              GNSProtocol.BAD_RESPONSE.toString() + " "
              + GNSProtocol.UPDATE_ERROR.toString() + " "
              + "Unable to update account info");
    }
  }


  public static CommandResponse resetPublicKey(InternalRequestHeader header,
          CommandPacket commandPacket,
          String guid, String password,
          String publicKey, ClientRequestHandlerInterface handler) {
    AccountInfo accountInfo;
    if ((accountInfo = lookupAccountInfoFromGuidLocally(header, guid, handler)) == null) {
      return new CommandResponse(ResponseCode.BAD_ACCOUNT_ERROR,
              GNSProtocol.BAD_RESPONSE.toString() + " "
              + GNSProtocol.BAD_ACCOUNT.toString() + " "
              + "Not an account guid");
    } else if (!accountInfo.isVerified()) {
      return new CommandResponse(ResponseCode.VERIFICATION_ERROR,
              GNSProtocol.BAD_RESPONSE.toString() + " "
              + GNSProtocol.VERIFICATION_ERROR.toString()
              + " Account not verified");
    }
    if (!password.equals(accountInfo.getPassword())) {
      return new CommandResponse(ResponseCode.VERIFICATION_ERROR,
              GNSProtocol.BAD_RESPONSE.toString() + " "
              + GNSProtocol.VERIFICATION_ERROR.toString() + " "
              + "Password mismatch");
    }
    GuidInfo guidInfo;
    if ((guidInfo = lookupGuidInfoLocally(header, guid, handler)) == null) {
      return new CommandResponse(ResponseCode.BAD_ACCOUNT_ERROR,
              GNSProtocol.BAD_RESPONSE.toString() + " "
              + GNSProtocol.BAD_ACCOUNT.toString() + " "
              + "Unable to read guid info");
    } else {
      guidInfo.setPublicKey(publicKey);
      guidInfo.noteUpdate();
      if (updateGuidInfoNoAuthentication(header, commandPacket,
              guidInfo, handler)) {
        return new CommandResponse(ResponseCode.NO_ERROR,
                GNSProtocol.OK_RESPONSE.toString() + " "
                + "Public key has been updated.");
      } else {
        return new CommandResponse(ResponseCode.UPDATE_ERROR,
                GNSProtocol.BAD_RESPONSE.toString() + " "
                + GNSProtocol.UNSPECIFIED_ERROR.toString()
                + " " + "Unable to update guid info");
      }
    }
  }


  public static CommandResponse addAccountInternal(
          InternalRequestHeader header, String name, String guid,
          String publicKey, String password, boolean emailVerify,
          String verifyCode, ClientRequestHandlerInterface handler)
          throws IOException {
    try {

      ResponseCode returnCode;
      // First try to createField the HRN record to make sure this name
      // isn't already registered
      JSONObject jsonHRN = new JSONObject();
      jsonHRN.put(HRN_GUID, guid);
      returnCode = handler.getInternalClient().createOrExists(
              new CreateServiceName(name, jsonHRN.toString()));

      String boundGUID = null;
      if (!returnCode.isExceptionOrError()
              || (guid.equals(boundGUID = HRNMatchingGUIDExists(header, handler, returnCode, name,
                      guid)))) {
        // if that's cool then add the entry that links the guid to the
        // username and public key
        // this one could fail if someone uses the same public key to
        // register another one... that's a nono
        // Note that password here is base64 encoded
        AccountInfo accountInfo = new AccountInfo(name, guid, password);
        accountInfo.noteUpdate();
        // if email verifications are off we just set it to verified
        if (!emailVerify) {
          accountInfo.setVerified(true);
        } else {
          accountInfo.setVerificationCode(verifyCode);
        }
        JSONObject json = new JSONObject();
        json.put(ACCOUNT_INFO, accountInfo.toJSONObject());
        GuidInfo guidInfo = new GuidInfo(name, guid, publicKey);
        json.put(GUID_INFO, guidInfo.toJSONObject());
        // set up ACL to look like this
        // "_GNS_ACL": {
        // "READ_WHITELIST": {"+ALL+": {"MD": "+ALL+"]}}}
        JSONObject acl = createACL(
                GNSProtocol.ENTIRE_RECORD.toString(),
                Arrays.asList(GNSProtocol.EVERYONE.toString()), null,
                null);
        // prefix is the same for all acls so just pick one to use here
        json.put(MetaDataTypeName.READ_WHITELIST.getPrefix(), acl);
        // set up the default read access

        returnCode = handler.getInternalClient().createOrExists(
                new CreateServiceName(guid, json.toString()));

        String boundHRN = null;
        assert (returnCode != null);
        if (!returnCode.isExceptionOrError()
                || name.equals(boundHRN = GUIDMatchingHRNExists(header, handler, returnCode,
                        name, guid))) // all good if here
        {
          return CommandResponse.noError();
        }

        if (returnCode.equals(ResponseCode.DUPLICATE_ID_EXCEPTION)) // try to delete the record we added above
        {
          return rollback(
                  handler,
                  ResponseCode.CONFLICTING_GUID_EXCEPTION
                  .setMessage(" Existing GUID "
                          + guid
                          + " has HRN "
                          + boundHRN
                          + " and can not be associated with the HRN "
                          + name), name, guid);
        }
      } else if (returnCode.equals(ResponseCode.DUPLICATE_FIELD_EXCEPTION) && !guid.equals(boundGUID)) {
        return new CommandResponse(
                ResponseCode.CONFLICTING_GUID_EXCEPTION,
                GNSProtocol.BAD_RESPONSE.toString()
                + " "
                + ResponseCode.CONFLICTING_GUID_EXCEPTION
                .getProtocolCode()
                + " "
                + name
                + "("
                + guid
                + ")"
                + " "
                + (returnCode.getMessage() != null ? returnCode
                        .getMessage() + " " : "") + "; HRN "
                + name + " is already bound to GUID "
                + boundGUID + " != " + guid);
      }

      // else the first HRN creation likely failed
      return new CommandResponse(returnCode,
              GNSProtocol.BAD_RESPONSE.toString() + " "
              + returnCode.getProtocolCode() + " " + name + "("
              + guid + ") " + returnCode.getMessage());
    } catch (JSONException e) {
      return CommandResponse.toCommandResponse(e);
    } catch (ClientException ce) {
      return new CommandResponse(
              ce.getCode(),
              GNSProtocol.BAD_RESPONSE.toString()
              + " "
              + ce.getCode()
              + " "
              + name
              + " "
              + ce.getMessage()
              + " ("
              + name
              + " may have gotten created despite this exception)");
    }
  }

  private static String HRNMatchingGUIDExists(InternalRequestHeader header,
          ClientRequestHandlerInterface handler, ResponseCode code,
          String name, String guid) throws ClientException, JSONException {
    String remoteRead = null;
    try {
      if (code.equals(ResponseCode.DUPLICATE_ID_EXCEPTION)) {
        if (guid.equals(remoteRead
                = handler.getInternalClient().execute(
                        GNSCommandInternal.fieldRead(
                                name,
                                InternalField
                                .makeInternalFieldString(GNSProtocol.GUID
                                        .toString()), header))
                // if HRN exists, returned map can not be null
                .getResultMap().values().iterator().next().toString())) {
          return remoteRead;
        }
      }
    } catch (IOException | InternalRequestException e) {
      throw new ClientException(ResponseCode.UNSPECIFIED_ERROR,
              e.getMessage(), e);
    }
    return remoteRead;
  }

  private static String GUIDMatchingHRNExists(InternalRequestHeader header,
          ClientRequestHandlerInterface handler, ResponseCode code,
          String name, String guid) throws ClientException, JSONException {
    try {
      if (code.equals(ResponseCode.DUPLICATE_ID_EXCEPTION)) {
        return handler.getInternalClient()
                .execute(
                        GNSCommandInternal.fieldRead(guid, HRN_FIELD,
                                header))
                .getResultMap().get(HRN_FIELD).toString();
      }
    } catch (IOException | InternalRequestException e) {
      throw new ClientException(ResponseCode.UNSPECIFIED_ERROR,
              e.getMessage(), e);
    }
    return null;
  }


  private static CommandResponse rollback(
          ClientRequestHandlerInterface handler, ResponseCode returnCode,
          String name, String guid) throws ClientException {
    ResponseCode rollbackCode = handler.getInternalClient().sendRequest(new DeleteServiceName(name));
    return new CommandResponse(
            returnCode,
            GNSProtocol.BAD_RESPONSE.toString()
            + " "
            + returnCode.getProtocolCode()
            + " "
            + guid
            + " "
            + returnCode.getMessage()
            + " "
            + (rollbackCode == null || !rollbackCode.isOKResult() ? "; failed to roll back "
                    + name
                    + " creation: "
                    + rollbackCode
                    + ":"
                    + rollbackCode.getMessage()
                    : "; rolled back " + name + " creation"));

  }


  public static CommandResponse removeAccount(InternalRequestHeader header,
          CommandPacket commandPacket,
          AccountInfo accountInfo, ClientRequestHandlerInterface handler) {
    // Step 1 - remove any group links
    ResponseCode removedGroupLinksResponseCode;
    try {
      removedGroupLinksResponseCode = GroupAccess.removeGuidFromGroups(header, commandPacket, accountInfo.getGuid(), handler);
    } catch (ClientException e) {
      removedGroupLinksResponseCode = e.getCode();
    } catch (IOException | InternalRequestException | JSONException e) {
      removedGroupLinksResponseCode = ResponseCode.UPDATE_ERROR;
    }
    // Step 2 - delete all the aliases records for this account
    ResponseCode deleteAliasesResponseCode = ResponseCode.NO_ERROR;
    for (String alias : accountInfo.getAliases()) {
      ResponseCode responseCode;
      try {
        responseCode = handler.getInternalClient().deleteOrNotExists(alias, true);
      } catch (ClientException e) {
        responseCode = e.getCode();
      }
      if (responseCode.isExceptionOrError()) {
        deleteAliasesResponseCode = ResponseCode.UPDATE_ERROR;
      }
    }
    // Step 3 - delete all the subGuids
    ResponseCode deleteSubGuidsResponseCode = ResponseCode.NO_ERROR;
    for (String subguid : accountInfo.getGuids()) {
      GuidInfo subGuidInfo = lookupGuidInfoAnywhere(header, subguid, handler);
      if (subGuidInfo != null && removeGuidInternal(header, commandPacket, subGuidInfo, accountInfo, true,
              handler).getExceptionOrErrorCode().isExceptionOrError()) {
        deleteSubGuidsResponseCode = ResponseCode.UPDATE_ERROR;
      }
    }
    // Step 4 - delete the HRN record
    ResponseCode deleteNameResponseCode;
    try {
      deleteNameResponseCode = handler.getInternalClient()
              .deleteOrNotExists(accountInfo.getName(), true);
    } catch (ClientException e) {
      deleteNameResponseCode = e.getCode();
    }

    if ((removedGroupLinksResponseCode.isExceptionOrError()
            || deleteAliasesResponseCode.isExceptionOrError())
            || deleteSubGuidsResponseCode.isExceptionOrError()
            || deleteNameResponseCode.isExceptionOrError()) {

      // Don't really care who caused the error, other than for debugging.
      return new CommandResponse(ResponseCode.UPDATE_ERROR,
              GNSProtocol.BAD_RESPONSE.toString()
              + " "
              + (removedGroupLinksResponseCode.isOKResult() ? "" : "; failed to remove links")
              + (deleteAliasesResponseCode.isOKResult() ? "" : "; failed to remove aliases")
              + (deleteSubGuidsResponseCode.isOKResult() ? "" : "; failed to remove subguids")
              + (deleteNameResponseCode.isOKResult() ? "" : "failed to delete " + accountInfo.getName())
      );
    } else {
      // Step 4 - If all the above stuff worked we delete the account guid record
      ResponseCode deleteGuidResponseCode;
      try {
        deleteGuidResponseCode = handler.getInternalClient()
                .deleteOrNotExists(accountInfo.getGuid(), true);
      } catch (ClientException e) {
        return new CommandResponse(e.getCode(),
                GNSProtocol.BAD_RESPONSE.toString()
                + " Failed to delete " + accountInfo.getGuid());
      }
      if (deleteGuidResponseCode.isOKResult()) {
        return new CommandResponse(ResponseCode.NO_ERROR,
                GNSProtocol.OK_RESPONSE.toString());
      } else {
        return new CommandResponse(deleteGuidResponseCode,
                GNSProtocol.BAD_RESPONSE.toString()
                + " Failed to delete " + accountInfo.getGuid());
      }
    }
  }


  public static CommandResponse addGuid(InternalRequestHeader header,
          CommandPacket commandPacket,
          AccountInfo accountInfo, GuidInfo accountGuidInfo, String name,
          String guid, String publicKey, ClientRequestHandlerInterface handler) {


    boolean createdName = false, createdGUID = false;
    try {
      JSONObject jsonHRN = new JSONObject();
      jsonHRN.put(HRN_GUID, guid);
      ResponseCode code;

      code = handler.getInternalClient().createOrExists(
              new CreateServiceName(name, jsonHRN.toString()));


      String boundGUID = null;
      if (code.equals(ResponseCode.DUPLICATE_ID_EXCEPTION)
              && !(guid.equals(boundGUID = HRNMatchingGUIDExists(header, handler,
                      code, name, guid)))) {
        return new CommandResponse(
                ResponseCode.CONFLICTING_GUID_EXCEPTION,
                GNSProtocol.BAD_RESPONSE.toString()
                + " "
                + ResponseCode.CONFLICTING_GUID_EXCEPTION
                .getProtocolCode()
                + " "
                + name
                + "("
                + guid
                + ")"
                + " "
                + (code.getMessage() != null ? code
                        .getMessage() + " " : "") + "; HRN "
                + name + " is already bound to GUID "
                + boundGUID + " != " + guid);
      }

      if (code.isExceptionOrError()
              && !code.equals(ResponseCode.DUPLICATE_ID_EXCEPTION)) {
        return new CommandResponse(code,
                GNSProtocol.BAD_RESPONSE.toString() + " "
                + code.getProtocolCode() + " " + name + "("
                + guid + ")" + " " + code.getMessage());
      }

      assert (!code.isExceptionOrError() || guid.equals(boundGUID));

      createdName = true;
      // else name created
      GuidInfo guidInfo = new GuidInfo(name, guid, publicKey);
      JSONObject jsonGuid = new JSONObject();
      jsonGuid.put(GUID_INFO, guidInfo.toJSONObject());
      jsonGuid.put(PRIMARY_GUID, accountInfo.getGuid());
      // set up ACL to look like this
      // "_GNS_ACL": {
      // "READ_WHITELIST": {"+ALL+": {"MD": [<publickey>, "+ALL+"]}},
      // "WRITE_WHITELIST": {"+ALL+": {"MD": [<publickey>]}}
      JSONObject acl = createACL(GNSProtocol.ENTIRE_RECORD.toString(),
              Arrays.asList(GNSProtocol.EVERYONE.toString(),
                      accountGuidInfo.getPublicKey()),
              GNSProtocol.ENTIRE_RECORD.toString(),
              Arrays.asList(accountGuidInfo.getPublicKey()));
      // prefix is the same for all acls so just pick one to use here
      jsonGuid.put(MetaDataTypeName.READ_WHITELIST.getPrefix(), acl);
      // The addGuid needs to be rolled back if the second step fails.
      ResponseCode guidCode;
      guidCode = handler.getInternalClient().createOrExists(
              new CreateServiceName(guid, jsonGuid.toString()));

      assert (guidCode != null);
      String boundHRN = null;
      if (guidCode.equals(ResponseCode.DUPLICATE_ID_EXCEPTION)
              && !name.equals(boundHRN = GUIDMatchingHRNExists(header, handler,
                      guidCode, name, guid))) // rollback name creation
      {
        return rollback(
                handler,
                ResponseCode.CONFLICTING_GUID_EXCEPTION
                .setMessage(": Existing GUID "
                        + guid
                        + " is associated with "
                        + boundHRN
                        + " and can not be associated with the HRN "
                        + name), name, guid);
      }

      // redundant to check with GNSClientInternal
      if (guidCode.isExceptionOrError()
              && !guidCode.equals(ResponseCode.DUPLICATE_ID_EXCEPTION)) {
        return new CommandResponse(guidCode,
                GNSProtocol.BAD_RESPONSE.toString() + " "
                + guidCode.getProtocolCode() + " "
                + guidCode.getMessage());
      }

      // else all good, continue
      assert (!guidCode.isExceptionOrError() || name.equals(boundHRN)) : "code="
              + guidCode
              + "; boundHRN="
              + boundHRN
              + "; name="
              + name
              + "; for GUID=" + guid;

      createdGUID = true;

      // else both name and guid created successfully
      updateAccountInfoNoAuthentication(header, commandPacket,
              accountInfo.addGuid(guid)
              .noteUpdate(), handler, true);

      return new CommandResponse(ResponseCode.NO_ERROR,
              GNSProtocol.OK_RESPONSE.toString() + " " + " [created "
              + name + " and " + guid
              + " and updated account info successfully]");
    } catch (ClientException ce) {
      return new CommandResponse(ce.getCode(),
              GNSProtocol.BAD_RESPONSE.toString()
              + " "
              + ce.getCode()
              + " "
              + ce.getMessage()
              + (createdName ? "; created "
                      + name
                      + (createdGUID ? "; created " + guid
                              + "; failed to update account info"
                              : "") : "; created neither " + name
                      + " nor " + guid));
    } catch (JSONException | ServerRuntimeException e) {
      return CommandResponse.toCommandResponse(e);
    }
  }


  public static CommandResponse addMultipleGuids(
          InternalRequestHeader header,
          CommandPacket commandPacket,
          List<String> names,
          List<String> publicKeys, AccountInfo accountInfo,
          GuidInfo accountGuidInfo, ClientRequestHandlerInterface handler) {
    try {
      long startTime = System.currentTimeMillis();
      Set<String> guids = new HashSet<>();
      Map<String, JSONObject> hrnMap = new HashMap<>();
      Map<String, JSONObject> guidInfoMap = new HashMap<>();
      for (int i = 0; i < names.size(); i++) {
        String name = names.get(i);
        String publicKey = publicKeys.get(i);
        String guid = SharedGuidUtils
                .createGuidStringFromBase64PublicKey(publicKey);
        accountInfo.addGuid(guid);
        guids.add(guid);
        // HRN records
        JSONObject jsonHRN = new JSONObject();
        jsonHRN.put(HRN_GUID, guid);
        hrnMap.put(name, jsonHRN);
        // guid info record
        GuidInfo guidInfo = new GuidInfo(name, guid, publicKey);
        JSONObject jsonGuid = new JSONObject();
        jsonGuid.put(GUID_INFO, guidInfo.toJSONObject());
        jsonGuid.put(PRIMARY_GUID, accountInfo.getGuid());
        // set up ACL to look like this
        // "_GNS_ACL": {
        // "READ_WHITELIST": {"+ALL+": {"MD": [<publickey>, "+ALL+"]}},
        // "WRITE_WHITELIST": {"+ALL+": {"MD": [<publickey>]}}
        JSONObject acl = createACL(
                GNSProtocol.ENTIRE_RECORD.toString(), Arrays.asList(
                        GNSProtocol.EVERYONE.toString(),
                        accountGuidInfo.getPublicKey()),
                GNSProtocol.ENTIRE_RECORD.toString(),
                Arrays.asList(accountGuidInfo.getPublicKey()));
        // prefix is the same for all acls so just pick one to use here
        jsonGuid.put(MetaDataTypeName.READ_WHITELIST.getPrefix(), acl);
        guidInfoMap.put(guid, jsonGuid);
      }
      DelayProfiler.updateDelay("addMultipleGuidsSetup", startTime);
      accountInfo.noteUpdate();

      // first we createField the HRN records as a batch
      ResponseCode returnCode;
      // First try to createField the HRNS to insure that that name does
      // not
      // already exist
      Map<String, String> nameStates = new HashMap<>();
      for (String key : hrnMap.keySet()) {
        nameStates.put(key, hrnMap.get(key).toString());
      }
      if (!(returnCode = handler.getInternalClient().createOrExists(new CreateServiceName(null, nameStates)))
              .isExceptionOrError()) {
        // now we update the account info
        if (updateAccountInfoNoAuthentication(header, commandPacket, accountInfo,
                handler, true).isOKResult()) {
          HashMap<String, String> guidInfoNameStates = new HashMap<>();
          for (String key : guidInfoMap.keySet()) {
            guidInfoNameStates.put(key, guidInfoMap.get(key).toString());
          }
          handler.getInternalClient().createOrExists(new CreateServiceName(null, guidInfoNameStates));

          GNSConfig.getLogger().info(DelayProfiler.getStats());
          return new CommandResponse(ResponseCode.NO_ERROR,
                  GNSProtocol.OK_RESPONSE.toString());
        }
      }
      return new CommandResponse(returnCode,
              GNSProtocol.BAD_RESPONSE.toString() + " "
              + returnCode.getProtocolCode() + " " + names);
    } catch (JSONException e) {
      return new CommandResponse(ResponseCode.JSON_PARSE_ERROR,
              GNSProtocol.BAD_RESPONSE.toString() + " "
              + GNSProtocol.JSON_PARSE_ERROR.toString() + " "
              + e.getMessage());
    } catch (ServerRuntimeException | ClientException e) {
      return new CommandResponse(ResponseCode.UNSPECIFIED_ERROR,
              GNSProtocol.BAD_RESPONSE.toString() + " "
              + GNSProtocol.UNSPECIFIED_ERROR.toString() + " "
              + e.getMessage());
    }
  }


  public static CommandResponse addMultipleGuidsFaster(
          InternalRequestHeader header, CommandPacket commandPacket,
          List<String> names,
          AccountInfo accountInfo, GuidInfo accountGuidInfo,
          ClientRequestHandlerInterface handler) {
    List<String> publicKeys = new ArrayList<>();
    for (String name : names) {
      String publicKey = "P" + name;
      publicKeys.add(publicKey);
    }
    return addMultipleGuids(header, commandPacket, names, publicKeys, accountInfo,
            accountGuidInfo, handler);
  }


  public static CommandResponse addMultipleGuidsFasterAllRandom(
          InternalRequestHeader header, CommandPacket commandPacket,
          int count, AccountInfo accountInfo,
          GuidInfo accountGuidInfo, ClientRequestHandlerInterface handler) {
    List<String> names = new ArrayList<>();
    List<String> publicKeys = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      String name = "N" + RandomString.randomString(10);
      names.add(name);
      String publicKey = "P" + name;
      publicKeys.add(publicKey);
    }
    return addMultipleGuids(header, commandPacket, names, publicKeys, accountInfo,
            accountGuidInfo, handler);
  }


  public static CommandResponse removeGuid(InternalRequestHeader header,
          CommandPacket commandPacket,
          GuidInfo guid, AccountInfo accountInfo,
          ClientRequestHandlerInterface handler) {
    return removeGuidInternal(header, commandPacket, guid, accountInfo, false, handler);
  }


  // This can be called from the context of an account guid deleting one if it's
  // subguids or a guid deleting itself. The difference being who signs the command,
  // but that's outside of this function
  private static CommandResponse removeGuidInternal(InternalRequestHeader header,
          CommandPacket commandPacket,
          GuidInfo guidInfo, AccountInfo accountInfo,
          boolean ignoreAccountGuid, ClientRequestHandlerInterface handler) {
    GNSConfig.getLogger().log(Level.FINE,
            "REMOVE: GUID INFO: {0} ACCOUNT INFO: {1}",
            new Object[]{guidInfo, accountInfo});
    // First make sure guid is not an account GUID
    // (unless we're sure it's not because we're deleting an account guid)
    if (!ignoreAccountGuid) {
      if (lookupAccountInfoFromGuidAnywhere(header, guidInfo.getGuid(), handler) != null) {
        return new CommandResponse(ResponseCode.BAD_GUID_ERROR,
                GNSProtocol.BAD_RESPONSE.toString() + " "
                + GNSProtocol.BAD_GUID.toString() + " "
                + guidInfo.getGuid() + " is an account guid");
      }
    }
    // Fill in a missing account info
    if (accountInfo == null) {
      String accountGuid = AccountAccess.lookupPrimaryGuid(header,
              guidInfo.getGuid(), handler, true);
      // should not happen unless records got messed up in GNS
      if (accountGuid == null) {
        return new CommandResponse(ResponseCode.BAD_ACCOUNT_ERROR,
                GNSProtocol.BAD_RESPONSE.toString() + " "
                + GNSProtocol.BAD_ACCOUNT.toString() + " "
                + guidInfo.getGuid()
                + " does not have a primary account guid");
      }
      if ((accountInfo = lookupAccountInfoFromGuidAnywhere(header, accountGuid,
              handler)) == null) {
        return new CommandResponse(ResponseCode.BAD_ACCOUNT_ERROR,
                GNSProtocol.BAD_RESPONSE.toString() + " "
                + GNSProtocol.BAD_ACCOUNT.toString() + " "
                + guidInfo.getGuid()
                + " cannot find primary account guid for "
                + accountGuid);
      }
    }

    // Step 1 - remove any group links
    ResponseCode removedGroupLinksResponseCode;
    try {
      removedGroupLinksResponseCode = GroupAccess.removeGuidFromGroups(
              header, commandPacket, guidInfo.getGuid(), handler);
    } catch (IOException | InternalRequestException | JSONException e) {
      removedGroupLinksResponseCode = ResponseCode.UPDATE_ERROR;
    } catch (ClientException e) {
      removedGroupLinksResponseCode = e.getCode();
    }
    // Step 2 - update the account info record unless this is part of an account guid delete
    ResponseCode accountInfoResponseCode;
    if (!ignoreAccountGuid) {
      accountInfo.removeGuid(guidInfo.getGuid());
      accountInfo.noteUpdate();
      accountInfoResponseCode = updateAccountInfoNoAuthentication(header, commandPacket,
              accountInfo,
              handler, true);
    } else {
      accountInfoResponseCode = ResponseCode.NO_ERROR;
    }
    // Step 3 - delete the HRN record
    ResponseCode deleteNameResponseCode;
    try {
      deleteNameResponseCode = handler.getInternalClient()
              .deleteOrNotExists(guidInfo.getName(), true);
    } catch (ClientException e) {
      deleteNameResponseCode = e.getCode();
    }
    if ((removedGroupLinksResponseCode.isExceptionOrError()
            || accountInfoResponseCode.isExceptionOrError())
            || deleteNameResponseCode.isExceptionOrError()) {
      // Don't really care who caused the error, other than for debugging.
      return new CommandResponse(ResponseCode.UPDATE_ERROR,
              GNSProtocol.BAD_RESPONSE.toString()
              + " "
              + (removedGroupLinksResponseCode.isOKResult() ? "" : "; failed to remove group links")
              + (accountInfoResponseCode.isOKResult() ? "" : "; failed to update account info "
                      + accountInfo.getGuid())
              + (deleteNameResponseCode.isOKResult() ? "" : "; failed to delete " + guidInfo.getName())
      );
    } else {
      // Step 4 - If all the above stuff worked we delete the guid record
      ResponseCode deleteGuidResponseCode;
      try {
        deleteGuidResponseCode = handler.getInternalClient()
                .deleteOrNotExists(guidInfo.getGuid(), true);
      } catch (ClientException e) {
        return new CommandResponse(e.getCode(),
                GNSProtocol.BAD_RESPONSE.toString()
                + " Failed to delete " + guidInfo.getGuid());
      }
      if (deleteGuidResponseCode.isOKResult()) {
        return new CommandResponse(ResponseCode.NO_ERROR,
                GNSProtocol.OK_RESPONSE.toString());
      } else {
        return new CommandResponse(deleteGuidResponseCode,
                GNSProtocol.BAD_RESPONSE.toString()
                + " Failed to delete " + guidInfo.getGuid());
      }
    }
  }


  public static CommandResponse addAlias(InternalRequestHeader header,
          CommandPacket commandPacket,
          AccountInfo accountInfo, String alias, String writer,
          String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler) {
    // insure that that name does not already exist
    try {
      ResponseCode returnCode;
      JSONObject jsonHRN = new JSONObject();
      jsonHRN.put(HRN_GUID, accountInfo.getGuid());
      if ((returnCode
              = handler.getInternalClient().createOrExists(new CreateServiceName(alias, jsonHRN.toString()))).isExceptionOrError()) {
        // roll this back
        accountInfo.removeAlias(alias);
        return new CommandResponse(returnCode,
                GNSProtocol.BAD_RESPONSE.toString() + " "
                + returnCode.getProtocolCode() + " " + alias
                + " " + returnCode.getMessage());
      }
      accountInfo.addAlias(alias);
      accountInfo.noteUpdate();
      if (updateAccountInfo(header, commandPacket,
              accountInfo.getGuid(), accountInfo,
              writer, signature, message, timestamp, handler, true)
              .isExceptionOrError()) {
        // back out if we got an error
        handler.getInternalClient().deleteOrNotExists(alias, true);
        return new CommandResponse(ResponseCode.UPDATE_ERROR,
                GNSProtocol.BAD_RESPONSE.toString() + " "
                + GNSProtocol.UPDATE_ERROR.toString());
      } else {
        return new CommandResponse(ResponseCode.NO_ERROR,
                GNSProtocol.OK_RESPONSE.toString());
      }
    } catch (JSONException e) {
      return new CommandResponse(ResponseCode.JSON_PARSE_ERROR,
              GNSProtocol.BAD_RESPONSE.toString() + " "
              + GNSProtocol.JSON_PARSE_ERROR.toString() + " "
              + e.getMessage());
    } catch (ClientException ce) {
      return new CommandResponse(ce.getCode(),
              GNSProtocol.BAD_RESPONSE.toString() + " " + ce.getCode()
              + " " + alias + " " + ce.getMessage());

    }
  }


  public static CommandResponse removeAlias(InternalRequestHeader header,
          CommandPacket commandPacket,
          AccountInfo accountInfo, String alias, String writer,
          String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler) {

    GNSConfig.getLogger().log(Level.FINE, "ALIAS: {0} ALIASES:{1}",
            new Object[]{alias, accountInfo.getAliases()});
    if (!accountInfo.containsAlias(alias)) {
      return new CommandResponse(ResponseCode.BAD_ALIAS_EXCEPTION,
              GNSProtocol.BAD_RESPONSE.toString() + " "
              + GNSProtocol.BAD_ALIAS.toString());
    }
    // remove the GNSProtocol.NAME.toString() -- GUID record
    ResponseCode responseCode;
    try {
      if ((responseCode = handler.getInternalClient().deleteOrNotExists(alias, true))
              .isExceptionOrError()) {
        return new CommandResponse(responseCode,
                GNSProtocol.BAD_RESPONSE.toString() + " "
                + responseCode.getProtocolCode() + " "
                + responseCode.getMessage());
      }
    } catch (ClientException ce) {
      return new CommandResponse(ce.getCode(),
              GNSProtocol.BAD_RESPONSE.toString() + " " + ce.getCode()
              + " " + ce.getMessage());
    }
    // Now updated the account record
    accountInfo.removeAlias(alias);
    accountInfo.noteUpdate();
    if ((responseCode = updateAccountInfo(header,
            commandPacket,
            accountInfo.getGuid(),
            accountInfo, writer, signature, message, timestamp, handler,
            true)).isExceptionOrError()) {
      return new CommandResponse(responseCode,
              GNSProtocol.BAD_RESPONSE.toString() + " "
              + responseCode.getProtocolCode());
    }
    return new CommandResponse(ResponseCode.NO_ERROR,
            GNSProtocol.OK_RESPONSE.toString());
  }


  public static CommandResponse setPassword(InternalRequestHeader header,
          CommandPacket commandPacket,
          AccountInfo accountInfo, String password, String writer,
          String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler) {
    accountInfo.setPassword(password);
    accountInfo.noteUpdate();
    if (updateAccountInfo(header,
            commandPacket,
            accountInfo.getGuid(), accountInfo,
            writer, signature, message, timestamp, handler, false)
            .isExceptionOrError()) {
      return new CommandResponse(ResponseCode.UPDATE_ERROR,
              GNSProtocol.BAD_RESPONSE.toString() + " "
              + GNSProtocol.UPDATE_ERROR.toString());
    }
    return new CommandResponse(ResponseCode.NO_ERROR,
            GNSProtocol.OK_RESPONSE.toString());
  }

  private static ResponseCode updateAccountInfo(InternalRequestHeader header,
          CommandPacket commandPacket,
          String guid, AccountInfo accountInfo, String writer,
          String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler, boolean sendToReplica) {
    try {
      ResponseCode response;
      if (sendToReplica) {
        // We potentially need to send the update to different replica.
        try {
          handler.getInternalClient().execute(
                  GNSCommandInternal.fieldUpdate(guid, ACCOUNT_INFO,
                          accountInfo.toJSONObject(), header));
          response = ResponseCode.NO_ERROR;
        } catch (JSONException e) {
          GNSConfig.getLogger().log(Level.SEVERE,
                  "JSON parse error with remote query:{0}", e);
          response = ResponseCode.JSON_PARSE_ERROR;
        } catch (ClientException | IOException | InternalRequestException e) {
          GNSConfig.getLogger().log(Level.SEVERE,
                  "Problem with remote query:{0}", e);
          response = ResponseCode.UNSPECIFIED_ERROR;
        }
      } else {
        GNSConfig.getLogger().log(Level.FINE,
                "Updating locally for GUID {0}:{1}<-{1}",
                new Object[]{guid, ACCOUNT_INFO, accountInfo});
        // Do the update locally.
        JSONObject json = new JSONObject();
        json.put(ACCOUNT_INFO, accountInfo.toJSONObject());
        response = FieldAccess.updateUserJSON(header,
                commandPacket,
                guid, json,
                writer, signature, message, timestamp, handler);
      }
      return response;
    } catch (JSONException e) {
      GNSConfig.getLogger().log(Level.SEVERE,
              "Problem parsing account info:{0}", e);
      return ResponseCode.JSON_PARSE_ERROR;
    }
  }

  private static ResponseCode updateAccountInfoLocallyNoAuthentication(
          InternalRequestHeader header, CommandPacket commandPacket,
          AccountInfo accountInfo, ClientRequestHandlerInterface handler) {
    return updateAccountInfoNoAuthentication(header, commandPacket,
            accountInfo, handler,
            false);
  }

  private static ResponseCode updateAccountInfoNoAuthentication(
          InternalRequestHeader header, CommandPacket commandPacket,
          AccountInfo accountInfo,
          ClientRequestHandlerInterface handler, boolean remoteUpdate) {
    return updateAccountInfo(header, commandPacket, accountInfo.getGuid(), accountInfo,
            GNSProtocol.INTERNAL_QUERIER.toString(),
            null, null, null, handler,
            remoteUpdate);
  }

  private static ResponseCode updateGuidInfo(InternalRequestHeader header,
          CommandPacket commandPacket,
          GuidInfo guidInfo,
          String writer, String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler) {
    try {
      JSONObject json = new JSONObject();
      json.put(GUID_INFO, guidInfo.toJSONObject());
      ResponseCode response = FieldAccess.updateUserJSON(header,
              commandPacket,
              guidInfo.getGuid(), json, writer, signature, message,
              timestamp, handler);
      return response;
    } catch (JSONException e) {
      GNSConfig.getLogger().log(Level.SEVERE,
              "Problem parsing guid info:{0}", e);
      return ResponseCode.JSON_PARSE_ERROR;
    }
  }

  private static boolean updateGuidInfoNoAuthentication(InternalRequestHeader header,
          CommandPacket commandPacket,
          GuidInfo guidInfo,
          ClientRequestHandlerInterface handler) {

    return !updateGuidInfo(header, commandPacket, guidInfo,
            GNSProtocol.INTERNAL_QUERIER.toString(),
            null, null, null, handler).isExceptionOrError();
  }


  public static JSONObject createACL(String readField,
          List<String> readAcessors, String writeField,
          List<String> writeAcessors) throws JSONException {
    JSONObject result = new JSONObject();

    if (readField != null && readAcessors != null) {
      JSONArray readlist = new JSONArray(readAcessors);
      JSONObject mdReadList = new JSONObject();
      mdReadList.put("MD", readlist);
      JSONObject readWhiteList = new JSONObject();
      readWhiteList.put(readField, mdReadList);
      result.put("READ_WHITELIST", readWhiteList);
    }

    if (writeField != null && writeAcessors != null) {
      JSONArray writelist = new JSONArray(writeAcessors);
      JSONObject mdWriteList = new JSONObject();
      mdWriteList.put("MD", writelist);
      JSONObject writeWhiteList = new JSONObject();
      writeWhiteList.put(writeField, mdWriteList);
      result.put("WRITE_WHITELIST", writeWhiteList);
    }
    return result;
  }

  // test code

  public static void main(String[] args) {
    String name = "westy@cs.umass.edu";
    String hostPortString = "128.119.44.108:8080";

    String adminBody = String.format(ADMIN_BODY,
            Config.getGlobalString(GNSConfig.GNSC.APPLICATION_NAME), // 1$
            name, // 2$
            hostPortString, // 3$
            Config.getGlobalString(GNSConfig.GNSC.STATUS_URL)); // 4$
    System.out.println(adminBody);
    Email.email("GNS Account Notification",
            Config.getGlobalString(GNSConfig.GNSC.SUPPORT_EMAIL), adminBody);

  }
}
