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
 * Initial developer(s): Westy */
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnscommon.GNSResponseCode;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.GNSCommandProtocol;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.ALL_FIELDS;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_ACCOUNT;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_ALIAS;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_GUID;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_RESPONSE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.DUPLICATE_GUID;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.DUPLICATE_NAME;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.EVERYONE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.JSON_PARSE_ERROR;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.OK_RESPONSE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.UNSPECIFIED_ERROR;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.UPDATE_ERROR;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.VERIFICATION_ERROR;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnscommon.exceptions.server.ServerRuntimeException;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnscommon.utils.ByteUtils;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnsserver.utils.Email;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSFieldAccess;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

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

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Provides the basic interface to GNS accounts.
 * <p>
 * See {@link AccountInfo} for more details about accounts.
 * <p>
 * Some of the internal records used to maintain account information are as
 * follows:
 * <p>
 * GUID: "ACCOUNT_INFO" -- {account} for primary guid<br>
 * GUID: "GUID" -- GUID (primary) for secondary guid<br>
 * GUID: "GUID_INFO" -- {guid info}<br>
 * HRN: "GUID" -- GUID<br>
 * <p>
 * GUID = Globally Unique Identifier<br>
 * HRN = Human Readable Name<br>
 *
 * @author westy, arun
 */
public class AccountAccess {

  /**
   * Defines the field name in an account guid where account information is
   * stored.
   */
  public static final String ACCOUNT_INFO = InternalField.makeInternalFieldString("account_info");
  /**
   * Special case for guids which can only be read from client as a list
   */
  public static final String ACCOUNT_INFO_GUIDS = InternalField.makeInternalFieldString("guids");

  /**
   * Defines the field name in an HRN record (the reverse record) where guid
   * is stored.
   */
  public static final String HRN_GUID = InternalField.makeInternalFieldString("guid");

  /**
   * Defines the field name in the subguid where parent guid is stored.
   */
  public static final String PRIMARY_GUID = InternalField.makeInternalFieldString("primary_guid");

  /**
   * Defines the field name in the guid where guid info is stored.
   */
  public static final String GUID_INFO = InternalField.makeInternalFieldString("guid_info");

  private static MessageDigest messageDigest;

  static {
    try {
      messageDigest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      GNSConfig.getLogger().log(Level.SEVERE, "Unable to initialize for authentication:{0}", e);
    }
  }

  /**
   * Obtains the account info record for the given GUID if that GUID was used
   * to create an account. Only looks on the local server.
   *
   * @param guid
   * @param handler
   * @return
   */
  public static AccountInfo lookupAccountInfoFromGuidLocally(String guid,
          ClientRequestHandlerInterface handler) {
    return lookupAccountInfoFromGuid(guid, handler, false);
  }

  /**
   * Obtains the account info record for the given GUID if that GUID was used
   * to create an account. Will do a remote query if needed.
   *
   * @param guid
   * @param handler
   * @return
   */
  public static AccountInfo lookupAccountInfoFromGuidAnywhere(String guid,
          ClientRequestHandlerInterface handler) {
    return lookupAccountInfoFromGuid(guid, handler, true);
  }

  /**
   * Obtains the account info record for the given GUID if that GUID was used
   * to create an account.
   * <p>
   * GUID: "ACCOUNT_INFO" -- {account} for primary guid<br>
   * GUID: "GUID" -- GUID (primary) for secondary guid<br>
   * GUID: "GUID_INFO" -- {guid info}<br>
   * HRN: "GUID" -- GUID<br>
   * <p>
   * GUID = Globally Unique Identifier<br>
   * HRN = Human Readable Name<br>
   *
   * @param guid
   * @param handler
   * @param allowRemoteLookup
   * @return the account info record or null if it could not be found
   */
  private static AccountInfo lookupAccountInfoFromGuid(String guid,
          ClientRequestHandlerInterface handler, boolean allowRemoteLookup) {
    try {
      ValuesMap result = NSFieldAccess.lookupJSONFieldLocalNoAuth(null, guid,
              ACCOUNT_INFO, handler.getApp(), false);
      GNSConfig.getLogger().log(Level.FINE,
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
    GNSConfig.getLogger().log(Level.FINE, "ACCOUNT_INFO NOT FOUND for {0}", guid);

    GNSConfig.getLogger().log(Level.FINE, "ACCOUNT_INFO NOT FOUND for {0}", guid);

    if (allowRemoteLookup) {
      GNSConfig.getLogger().log(Level.FINE,
              "LOOKING REMOTELY for ACCOUNT_INFO for {0}",
              guid);
      String value = null;
      try {
        value = handler.getRemoteQuery().fieldRead(guid, ACCOUNT_INFO);
      } catch (IOException | JSONException | ClientException e) {
        // Do nothing as this is a normal result when the record doesn't
        // exist.
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

  /**
   * If this is a subguid associated with an account, returns the GUID of that
   * account, otherwise returns null.
   * <p>
   * GUID = Globally Unique Identifier
   *
   * @param guid
   * @param handler
   * @param allowRemoteLookup
   * @return a GUID
   */
  public static String lookupPrimaryGuid(String guid,
          ClientRequestHandlerInterface handler, boolean allowRemoteLookup) {
    try {
      ValuesMap result = NSFieldAccess.lookupJSONFieldLocalNoAuth(null, guid,
              PRIMARY_GUID, handler.getApp(), false);
      GNSConfig.getLogger().log(Level.FINE, "ValuesMap for {0} / {1}: {2}",
              new Object[]{guid, PRIMARY_GUID, result});
      if (result != null) {
        return result.getString(PRIMARY_GUID);
      }
    } catch (FailedDBOperationException | JSONException e) {
      GNSConfig.getLogger().log(Level.SEVERE, "Problem extracting PRIMARY_GUID from {0} :{1}",
              new Object[]{guid, e});
    }
    String value = null;
    GNSConfig.getLogger().log(Level.FINE, "PRIMARY_GUID NOT FOUND LOCALLY for {0}",
            guid);

    if (allowRemoteLookup) {
      GNSConfig.getLogger().log(Level.FINE,
              "LOOKING REMOTELY for PRIMARY_GUID for {0}",
              guid);
      try {
        value = handler.getRemoteQuery().fieldRead(guid, PRIMARY_GUID);
        if (!FieldAccess.SINGLE_FIELD_VALUE_ONLY && value != null) {
          value = new JSONObject(value).getString(PRIMARY_GUID);
        }
      } catch (IOException | JSONException | ClientException e) {
        GNSConfig.getLogger().log(Level.SEVERE,
                "Problem getting HRN_GUID for {0} from remote server: {1}",
                new Object[]{guid, e});
      }
    }
    return value;
  }

  /**
   * Returns the GUID associated with name which is a HRN or null if one of
   * that name does not exist. Will use a remote query if necessary.
   *
   * <p>
   * GUID = Globally Unique Identifier<br>
   * HRN = Human Readable Name<br>
   *
   * @param name
   * @param handler
   * @return a guid or null if the corresponding guid does not exist
   */
  public static String lookupGuidAnywhere(String name,
          ClientRequestHandlerInterface handler) {
    return lookupGuid(name, handler, true);
  }

  /**
   * Returns the GUID associated with name which is a HRN or null if one of
   * that name does not exist.
   *
   * <p>
   * GUID = Globally Unique Identifier<br>
   * HRN = Human Readable Name<br>
   *
   * @param name
   * @param handler
   * @return a guid or null if the corresponding guid does not exist
   */
  public static String lookupGuidLocally(String name,
          ClientRequestHandlerInterface handler) {
    return lookupGuid(name, handler, false);
  }

  /**
   * Returns the GUID associated with name which is a HRN or null if one of
   * that name does not exist.
   * <p>
   * GUID = Globally Unique Identifier<br>
   * HRN = Human Readable Name<br>
   *
   * @param name
   * @param handler
   * @param allowRemoteLookup
   * @return a guid or null if the corresponding guid does not exist
   */
  private static String lookupGuid(String name,
          ClientRequestHandlerInterface handler, boolean allowRemoteLookup) {
    try {
      ValuesMap result = NSFieldAccess.lookupJSONFieldLocalNoAuth(null, name,
              HRN_GUID, handler.getApp(), false);
      GNSConfig.getLogger().log(Level.FINE, "ValuesMap for {0} / {1}: {2}",
              new Object[]{name, HRN_GUID, result});
      if (result != null) {
        return result.getString(HRN_GUID);
      }
    } catch (FailedDBOperationException | JSONException e) {
      GNSConfig.getLogger().log(Level.SEVERE, "Problem extracting HRN_GUID from {0} :{1}",
              new Object[]{name, e});
    }
    //
    String value = null;
    GNSConfig.getLogger().log(Level.FINE, "HRN_GUID NOT FOUND for {0}", name);
    if (allowRemoteLookup) {
      GNSConfig.getLogger().log(Level.FINE, "LOOKING REMOTELY for HRN_GUID for {0}", name);
      try {
        value = handler.getRemoteQuery().fieldRead(name, HRN_GUID);
        if (!FieldAccess.SINGLE_FIELD_VALUE_ONLY && value != null) {
          GNSConfig.getLogger().log(Level.FINE, "Found HRN_GUID for {0}:{1}", new Object[]{name, value});
          value = new JSONObject(value).getString(HRN_GUID);
        }
      } catch (IOException | JSONException | ClientException e) {
        GNSConfig.getLogger().log(Level.SEVERE,
                "Problem getting HRN_GUID for {0} from remote server: {1}",
                new Object[]{name, e});
      }
    }
    return value;
  }

  /**
   * Obtains the guid info record from the database for GUID given.
   * <p>
   * GUID = Globally Unique Identifier<br>
   *
   * @param guid
   * @param handler
   * @return an {@link GuidInfo} instance
   */
  public static GuidInfo lookupGuidInfoLocally(String guid,
          ClientRequestHandlerInterface handler) {
    return lookupGuidInfo(guid, handler, false);
  }

  /**
   * Obtains the guid info record from the database for GUID given from
   * any server, local or remote.
   * <p>
   * GUID = Globally Unique Identifier<br>
   *
   * @param guid
   * @param handler
   * @return an {@link GuidInfo} instance
   */
  public static GuidInfo lookupGuidInfoAnywhere(String guid,
          ClientRequestHandlerInterface handler) {
    return lookupGuidInfo(guid, handler, true);
  }

  /**
   * Obtains the guid info record from the database for GUID given.
   * <p>
   * GUID = Globally Unique Identifier<br>
   *
   * @param guid
   * @param handler
   * @param allowRemoteLookup
   * @return an {@link GuidInfo} instance
   */
  private static GuidInfo lookupGuidInfo(String guid,
          ClientRequestHandlerInterface handler, boolean allowRemoteLookup) {
    GNSConfig.getLogger().log(Level.FINE, "allowRemoteLookup is {0}", allowRemoteLookup);
    try {
      ValuesMap result = NSFieldAccess.lookupJSONFieldLocalNoAuth(null, guid,
              GUID_INFO, handler.getApp(), false);
      GNSConfig.getLogger().log(Level.FINE, "ValuesMap for {0} / {1} {2}",
              new Object[]{guid, GUID_INFO, result != null ? result.getSummary() : result});
      if (result != null) {
        return new GuidInfo(new JSONObject(result.getString(GUID_INFO)));
      }
    } catch (FailedDBOperationException | JSONException | ParseException e) {
      GNSConfig.getLogger().log(Level.SEVERE,
              "Problem extracting GUID_INFO from {0} :{1}",
              new Object[]{guid, e});
    }

    GNSConfig.getLogger().log(Level.FINE,
            "GUID_INFO NOT FOUND for {0}", guid);
    if (allowRemoteLookup) {
      GNSConfig.getLogger().log(Level.FINE,
              "LOOKING REMOTELY for GUID_INFO for {0}",
              guid);
      String value = null;
      try {
        value = handler.getRemoteQuery().fieldRead(guid, GUID_INFO);
      } catch (IOException | JSONException | ClientException e) {
        GNSConfig.getLogger().log(Level.SEVERE,
                "Problem getting GUID_INFO for {0} from remote server: {1}",
                new Object[]{guid, e});
      }
      if (value != null) {
        try {
          return new GuidInfo(new JSONObject(value));
        } catch (JSONException | ParseException e) {
          GNSConfig.getLogger().log(Level.SEVERE,
                  "Problem parsing GUID_INFO value from remote server for {0}: {1}",
                  new Object[]{guid, e});
        }
      }
    }
    return null;
  }

  /**
   * Obtains the account info record from the database for the account whose
   * HRN is name. Will use a remote query if necessary.
   * <p>
   * HRN = Human Readable Name<br>
   *
   * @param name
   * @param handler
   * @return an {@link AccountInfo} instance
   */
  public static AccountInfo lookupAccountInfoFromNameAnywhere(String name,
          ClientRequestHandlerInterface handler) {
    String guid = lookupGuidAnywhere(name, handler);
    if (guid != null) {
      return lookupAccountInfoFromGuidAnywhere(guid, handler);
    }
    return null;
  }

  private static final String VERIFY_COMMAND = "account_verify";
  private static final String EMAIL_BODY
          = "Hi %2$s,\n\n"
          + "This is an automated message informing you that %1$s has created\n"
          + "an account for %2$s. You were sent this message to insure that the\n"
          + "person that created this account actually has access to this email address.\n\n"
          + "To verify this is your email address you can click on the link below.\n"
          + "If you are unable to click on the link, you can complete your email address\n"
          + "verification by copying and pasting the URL into your web browser:\n\n"
          + "http://%3$s/"
          + GNSConfig.GNS_URL_PATH
          + "/VerifyAccount?guid=%4$s&code=%5$s\n\n"
          + "If you did not create this account you can just ignore this email and nothing bad will happen.\n\n"
          + "Thank you,\nThe CASA Team.";
  private static final String EMAIL_CLI_CONDITIONAL
          = "\n\nFor GNS CLI users only: enter this command into the CLI that you used to create the account:\n\n"
          + VERIFY_COMMAND + " %1$s %2$s\n\n";

  private static final String SUCCESS_NOTICE = "A confirmation email has been sent to %s. "
          + "Please follow the instructions in that email to verify your account.\n";
  private static final String PROBLEM_NOTICE = "There is some system problem in sending "
          + "your confirmation email to %s. "
          + "Your account has been created. Please email us at %s and we will attempt to fix the problem.\n";
  //
  private static final String ADMIN_NOTICE = "This is an automated message informing "
          + "you that %1$s has created an account for %2$s on the GNS server at %3$s.\n"
          + "You can view their information using the link below:"
          // FIXME: this hack
          + "\n\n%4$%2$s?name\n";

  /**
   * Adds an account guid.
   *
   * @param hostPortString
   * @param name
   * @param guid
   * @param publicKey
   * @param password
   * @param handler
   * @return the command response
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws java.io.IOException
   * @throws org.json.JSONException
   */
  public static CommandResponse addAccountWithVerification(
          final String hostPortString, final String name, final String guid,
          String publicKey, String password,
          ClientRequestHandlerInterface handler) throws ClientException,
          IOException, JSONException {

    CommandResponse response;
    // make this even if  we don't need it
    String verifyCode = createVerificationCode(name);
    if ((response = addAccount(name, guid, publicKey, password,
            GNSConfig.GNSC.isEmailAuthenticationEnabled(),
            verifyCode, handler)).getExceptionOrErrorCode().isOKResult()) {

      // Account creation was succesful so maybe send email verification.
      if (GNSConfig.GNSC.isEmailAuthenticationEnabled()) {
        boolean emailSent = sendEmailAuthentication(name, guid, hostPortString, verifyCode);
        if (emailSent) {
          return new CommandResponse(GNSResponseCode.NO_ERROR, OK_RESPONSE);
        } else {
          // if we can't send the confirmation back out of the account creation
          AccountInfo accountInfo = lookupAccountInfoFromGuidAnywhere(guid, handler);
          if (accountInfo != null) {
            removeAccount(accountInfo, handler);
          }
          return new CommandResponse(
                  GNSResponseCode.VERIFICATION_ERROR, BAD_RESPONSE
                  + " " + VERIFICATION_ERROR + " "
                  + "Unable to send verification email");
        }
      } else {
        GNSConfig.getLogger().warning(
                "**** EMAIL VERIFICATION IS OFF! ****");
      }
    }
    return response;
  }

  private static final int VERIFICATION_CODE_LENGTH = 3; // Six hex characters

  private static final String SECRET = Config.getGlobalString(GNSConfig.GNSC.VERIFICATION_SECRET);

  private static String createVerificationCode(String name) {
    return ByteUtils.toHex(Arrays.copyOf(ShaOneHashFunction
            .getInstance().hash(name + SECRET
                    // Add salt unless email verification is disabled or salt is disabled.
                    + (GNSConfig.GNSC.isEmailAuthenticationEnabled()
                    && GNSConfig.GNSC.isEmailAuthenticationSaltEnabled()
                            ? new String(
                                    Util.getRandomAlphanumericBytes(128))
                            : "")),
            VERIFICATION_CODE_LENGTH));
  }

  private static boolean sendEmailAuthentication(String name, String guid, String hostPortString, String verifyCode) {
    //if (GNSConfig.enableEmailAccountVerification) {
    // Send out the confirmation email with a verification code
    String emailBody = String.format(EMAIL_BODY,
            Config.getGlobalString(GNSConfig.GNSC.APPLICATION_NAME), //1$
            name, //2$
            hostPortString, //3$
            guid, //4$
            verifyCode //5$
    );
    if (Config.getGlobalBoolean(GNSConfig.GNSC.INCLUDE_CLI_NOTIFICATION)) {
      emailBody += String.format(EMAIL_CLI_CONDITIONAL, name, verifyCode);
    }
    String subject = Config.getGlobalString(GNSConfig.GNSC.APPLICATION_NAME)
            + " Account Authentication";
    boolean emailOK = Email.email(subject, name, emailBody);
    // do the admin email in another thread so it's faster and
    // because we don't care if it completes
    (new Thread() {
      @Override
      public void run() {
        boolean adminEmailOK = Email.email("GNS Account Notification",
                Config.getGlobalString(GNSConfig.GNSC.SUPPORT_EMAIL),
                String.format(ADMIN_NOTICE,
                        Config.getGlobalString(GNSConfig.GNSC.APPLICATION_NAME), // 1$
                        name, // 2$
                        hostPortString, // 3$
                        Config.getGlobalString(GNSConfig.GNSC.STATUS_URL) //4$ 
                ));
      }
    }).start();
    return emailOK;
  }

  public static CommandResponse resendAuthenticationEmail(AccountInfo accountInfo,
          String guid, String signature, String message,
          ClientRequestHandlerInterface handler) throws UnknownHostException {
    if (GNSConfig.GNSC.isEmailAuthenticationEnabled()) {
      String name = accountInfo.getName();
      String code = createVerificationCode(name);
      boolean emailSent = sendEmailAuthentication(name, guid, handler.getHTTPServerHostPortString(), code);
      if (emailSent) {
        accountInfo.setVerificationCode(code);
        accountInfo.noteUpdate();
        if (updateAccountInfoNoAuthentication(accountInfo, handler, false)) {
          return new CommandResponse(GNSResponseCode.NO_ERROR,
                  GNSCommandProtocol.OK_RESPONSE);
        } else {
          return new CommandResponse(GNSResponseCode.UPDATE_ERROR,
                  GNSCommandProtocol.BAD_RESPONSE + " "
                  + GNSCommandProtocol.UPDATE_ERROR + " "
                  + "Unable to update account info");
        }
      } else {
        return new CommandResponse(GNSResponseCode.VERIFICATION_ERROR, BAD_RESPONSE
                + " " + VERIFICATION_ERROR + " " + "Unable to send verification email");
      }
    }
    return new CommandResponse(GNSResponseCode.VERIFICATION_ERROR, BAD_RESPONSE
            + " " + VERIFICATION_ERROR + " " + "Email verification is disabled.");
  }

  /**
   * Performs the account verification for a given guid using the verification
   * code.
   *
   * @param guid
   * @param code
   * @param handler
   * @return the command response
   */
  public static CommandResponse verifyAccount(String guid, String code,
          ClientRequestHandlerInterface handler) {
    AccountInfo accountInfo;
    if ((accountInfo = lookupAccountInfoFromGuidLocally(guid, handler)) == null) {
      return new CommandResponse(GNSResponseCode.VERIFICATION_ERROR,
              GNSCommandProtocol.BAD_RESPONSE + " "
              + GNSCommandProtocol.VERIFICATION_ERROR + " "
              + "Unable to read account info");
    }
    if (accountInfo.isVerified()) {
      return new CommandResponse(
              GNSResponseCode.ALREADY_VERIFIED_EXCEPTION,
              GNSCommandProtocol.BAD_RESPONSE + " "
              + GNSCommandProtocol.ALREADY_VERIFIED_EXCEPTION
              + " Account already verified");
    }
    if (accountInfo.getVerificationCode() == null && code == null) {
      return new CommandResponse(GNSResponseCode.VERIFICATION_ERROR,
              GNSCommandProtocol.BAD_RESPONSE + " "
              + GNSCommandProtocol.VERIFICATION_ERROR + " "
              + "Bad verification code");
    }
    if (accountInfo.getCodeTime() == null) {
      return new CommandResponse(GNSResponseCode.VERIFICATION_ERROR,
              GNSCommandProtocol.BAD_RESPONSE + " "
              + GNSCommandProtocol.VERIFICATION_ERROR + " "
              + "Cannot retrieve account code time");
    }
    if ((new Date()).getTime() - accountInfo.getCodeTime().getTime()
            > Config.getGlobalInt(GNSConfig.GNSC.EMAIL_VERIFICATION_TIMEOUT_IN_HOURS) * 60 * 60 * 1000) {
      return new CommandResponse(GNSResponseCode.VERIFICATION_ERROR,
              GNSCommandProtocol.BAD_RESPONSE + " "
              + GNSCommandProtocol.VERIFICATION_ERROR + " "
              + "Account code no longer valid");
    }
    if (!accountInfo.getVerificationCode().equals(code)) {
      return new CommandResponse(GNSResponseCode.VERIFICATION_ERROR,
              GNSCommandProtocol.BAD_RESPONSE + " "
              + GNSCommandProtocol.VERIFICATION_ERROR + " "
              + "Code not correct");
    }
    accountInfo.setVerificationCode(null);
    accountInfo.setVerified(true);
    accountInfo.noteUpdate();
    if (updateAccountInfoNoAuthentication(accountInfo, handler, false)) {
      return new CommandResponse(GNSResponseCode.NO_ERROR,
              GNSCommandProtocol.OK_RESPONSE + " "
              + "Your account has been verified."); // add a
      // little something for the kids
    } else {
      return new CommandResponse(GNSResponseCode.UPDATE_ERROR,
              GNSCommandProtocol.BAD_RESPONSE + " "
              + GNSCommandProtocol.UPDATE_ERROR + " "
              + "Unable to update account info");
    }
  }

  /**
   * Reset the public key for an account guid.
   *
   * @param guid
   * @param password
   * @param publicKey
   * @param handler
   * @return the command response
   */
  public static CommandResponse resetPublicKey(String guid, String password,
          String publicKey, ClientRequestHandlerInterface handler) {
    AccountInfo accountInfo;
    if ((accountInfo = lookupAccountInfoFromGuidLocally(guid, handler)) == null) {
      return new CommandResponse(GNSResponseCode.BAD_ACCOUNT_ERROR, GNSCommandProtocol.BAD_RESPONSE + " "
              + GNSCommandProtocol.BAD_ACCOUNT + " " + "Not an account guid");
    } else if (!accountInfo.isVerified()) {
      return new CommandResponse(GNSResponseCode.VERIFICATION_ERROR,
              BAD_RESPONSE + " " + VERIFICATION_ERROR
              + " Account not verified");
    }
    if (verifyPassword(accountInfo, password)) {
      GuidInfo guidInfo;
      if ((guidInfo = lookupGuidInfoLocally(guid, handler)) == null) {
        return new CommandResponse(GNSResponseCode.BAD_ACCOUNT_ERROR,
                GNSCommandProtocol.BAD_RESPONSE + " "
                + GNSCommandProtocol.BAD_ACCOUNT + " "
                + "Unable to read guid info");
      } else {
        guidInfo.setPublicKey(publicKey);
        guidInfo.noteUpdate();
        if (updateGuidInfoNoAuthentication(guidInfo, handler)) {
          return new CommandResponse(GNSResponseCode.NO_ERROR,
                  GNSCommandProtocol.OK_RESPONSE + " "
                  + "Public key has been updated.");
        } else {
          return new CommandResponse(GNSResponseCode.UPDATE_ERROR,
                  GNSCommandProtocol.BAD_RESPONSE + " "
                  + GNSCommandProtocol.UNSPECIFIED_ERROR
                  + " " + "Unable to update guid info");
        }
      }
    } else {
      return new CommandResponse(GNSResponseCode.VERIFICATION_ERROR,
              GNSCommandProtocol.BAD_RESPONSE + " "
              + GNSCommandProtocol.VERIFICATION_ERROR + " "
              + "Password mismatch");
    }
  }

  // synchronized use of the MessageDigest
  private static synchronized boolean verifyPassword(AccountInfo accountInfo,
          String password) {
    try {
      messageDigest.update((password + SALT + accountInfo
              .getName()).getBytes("UTF-8"));
      return accountInfo.getPassword().equals(
              encryptPassword(password, accountInfo.getName()));
    } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
      GNSConfig.getLogger().log(Level.WARNING,
              "Problem hashing password:{0}", e);
      return false;
    }
  }

  // Code is duplicated in all of the clients (currently java, php, iphone).
  // function encryptPassword($password, $username) {
  // return base64_encode(hash('sha256', $password . "42shabiz" . $username,
  // true));
  // }
  private static final String SALT = "42shabiz";

  // synchronized use of the MessageDigest
  private static synchronized String encryptPassword(String password,
          String alias) throws NoSuchAlgorithmException,
          UnsupportedEncodingException {
    messageDigest.update((password + SALT + alias).getBytes("UTF-8"));
    return Base64.encodeToString(messageDigest.digest(), false);
  }

  /**
   * Create a new GNS user account.
   *
   * THIS CAN BYPASS THE EMAIL VERIFICATION if you set emailVerify to false;
   *
   * <p>
   * This adds three records to the GNS for the account:<br>
   * NAME: "_GNS_GUID" -- guid<br>
   * GUID: "_GNS_ACCOUNT_INFO" -- {account record - an AccountInfo object
   * stored as JSON}<br>
   * GUID: "_GNS_GUID_INFO" -- {guid record - a GuidInfo object stored as
   * JSON}<br>
   *
   * @param name
   * @param guid
   * @param publicKey
   * @param password
   * @param emailVerify
   * @param verifyCode
   * @param handler
   * @return status result
   */
  public static CommandResponse addAccount(String name, String guid,
          String publicKey, String password, boolean emailVerify,
          String verifyCode, ClientRequestHandlerInterface handler) {
    try {

      GNSResponseCode returnCode;
      // First try to create the HRN record to make sure this name isn't
      // already registered
      JSONObject jsonHRN = new JSONObject();
      jsonHRN.put(HRN_GUID, guid);
      if (!(returnCode = handler.getRemoteQuery().createRecord(name,
              jsonHRN)).isExceptionOrError()) {
        // if that's cool then add the entry that links the GUID to the
        // username and public key
        // this one could fail if someone uses the same public key to
        // register another one... that's a nono
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
        JSONObject acl = createACL(ALL_FIELDS, Arrays.asList(EVERYONE),
                null, null);
        // prefix is the same for all acls so just pick one to use here
        json.put(MetaDataTypeName.READ_WHITELIST.getPrefix(), acl);
        // set up the default read access

        returnCode = null;
        try {
          returnCode = handler.getRemoteQuery().createRecord(guid,
                  json);
        } catch (ClientException ce1) {
          (returnCode = ce1.getCode()).setMessage(ce1.getMessage());
        }
        if (returnCode != null && !returnCode.isExceptionOrError()) {
          return new CommandResponse(GNSResponseCode.NO_ERROR,
                  OK_RESPONSE);
        } else {
          // delete the record we added above
          // might be nice to have a notion of a transaction that we
          // could roll back
          GNSResponseCode rollbackCode = handler.getRemoteQuery()
                  .deleteRecordSuppressExceptions(name);
          return new CommandResponse(
                  returnCode,
                  BAD_RESPONSE
                  + " "
                  + returnCode.getProtocolCode()
                  + " "
                  + guid
                  + " "
                  + returnCode.getMessage()
                  + " "
                  + (rollbackCode == null
                  || !rollbackCode.isOKResult() ? "; failed to roll back "
                          + name
                          + " creation: "
                          + rollbackCode
                          + ":"
                          + rollbackCode.getMessage()
                          : "; rolled back " + name
                          + " creation"));
        }
      } else {
        return new CommandResponse(returnCode, BAD_RESPONSE + " "
                + returnCode.getProtocolCode() + " " + name + "("
                + guid + ") " + returnCode.getMessage());
      }
    } catch (JSONException e) {
      return new CommandResponse(GNSResponseCode.JSON_PARSE_ERROR,
              BAD_RESPONSE + " " + JSON_PARSE_ERROR + " "
              + e.getMessage());
    } catch (ClientException ce) {
      return new CommandResponse(ce.getCode(), BAD_RESPONSE + " "
              + ce.getCode() + " " + name + " " + ce.getMessage() + " ("
              + name + " may have gotten created despite this exception)");

    }
  }

  /**
   * Removes a GNS user account.
   *
   * @param accountInfo
   * @param handler
   * @return status result
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws java.io.IOException
   * @throws org.json.JSONException
   */
  public static CommandResponse removeAccount(AccountInfo accountInfo,
          ClientRequestHandlerInterface handler) throws ClientException,
          IOException, JSONException {
    boolean removedGroupLinks = false, deletedGUID = false, deletedName = false, deletedAliases = false;
    try {
      // First remove any group links
      GroupAccess.cleanupGroupsForDelete(accountInfo.getGuid(),
              handler);
      removedGroupLinks = true;
      // Then remove the HRN link
      if (!handler.getRemoteQuery().deleteRecordSuppressExceptions(
              accountInfo.getName()).isExceptionOrError()) {
        deletedName = true;
        handler.getRemoteQuery().deleteRecordSuppressExceptions(accountInfo.getGuid());
        deletedGUID = true;
        // remove all the alias reverse links
        for (String alias : accountInfo.getAliases()) {
          handler.getRemoteQuery().deleteRecordSuppressExceptions(
                  alias);
        }
        deletedAliases = true;
        // get rid of all subguids
        for (String subguid : accountInfo.getGuids()) {
          GuidInfo subGuidInfo = lookupGuidInfoAnywhere(subguid, handler);
          if (subGuidInfo != null) { // should not be null, ignore if it is
            removeGuid(subGuidInfo, accountInfo, true, handler);
          }
        }

        // all is well
        return new CommandResponse(GNSResponseCode.NO_ERROR,
                OK_RESPONSE);
      } else {
        return new CommandResponse(GNSResponseCode.BAD_ACCOUNT_ERROR,
                BAD_RESPONSE + " " + BAD_ACCOUNT);
      }
    } catch (ClientException ce) {
      return new CommandResponse(ce.getCode(), BAD_RESPONSE
              + " "
              + ce.getMessage()
              + (removedGroupLinks ? "; removed group links" : "")
              + (deletedName ? "; deleted "
                      + accountInfo.getName() : "")
              + (deletedGUID ? "; deleted "
                      + accountInfo.getGuid() : "")
              + (deletedAliases ? "; deleted "
                      + Util.truncatedLog(accountInfo.getAliases(), 16)
                      : "")
              + (deletedName ? "; deleted "
                      + Util.truncatedLog(accountInfo.getGuids(), 16)
                      : "") + "; failed to update account info "
              + accountInfo.getGuid());
    }
  }

  /**
   * Adds a new GUID associated with an existing account.
   * <p>
   * These records will be created:<br>
   * GUID: "_GNS_PRIMARY_GUID" -- GUID (primary) for secondary guid<br>
   * GUID: "_GNS_GUID_INFO" -- {guid info}<br>
   * HRN: "_GNS_GUID" -- GUID<br>
   *
   * @param accountInfo
   * - the accountInfo of the account to add the GUID to
   * @param accountGuidInfo
   * @param name
   * = the human readable name to associate with the GUID
   * @param guid
   * - the new GUID
   * @param publicKey
   * - the public key to use with the new account
   * @param handler
   * @return status result
   */
  public static CommandResponse addGuid(AccountInfo accountInfo,
          GuidInfo accountGuidInfo, String name, String guid,
          String publicKey, ClientRequestHandlerInterface handler) {
    if ((AccountAccess.lookupGuidAnywhere(name, handler)) != null) {
      return new CommandResponse(
              GNSResponseCode.DUPLICATE_NAME_EXCEPTION, BAD_RESPONSE
              + " " + DUPLICATE_NAME + " " + name);
    }
    if ((AccountAccess.lookupGuidInfoAnywhere(guid, handler)) != null) {
      return new CommandResponse(
              GNSResponseCode.DUPLICATE_GUID_EXCEPTION, BAD_RESPONSE
              + " " + DUPLICATE_GUID + " " + name);
    }
    boolean createdName = false, createdGUID = false;
    try {
      JSONObject jsonHRN = new JSONObject();
      jsonHRN.put(HRN_GUID, guid);
      GNSResponseCode code = handler.getRemoteQuery().createRecord(name, jsonHRN);
      // Return the error if we could not create the HRN (alias) record.
      if (code.isExceptionOrError()) {
        return new CommandResponse(code, BAD_RESPONSE + " "
                + code.getProtocolCode() + " " + name + "(" + guid
                + ")" + " " + code.getMessage());
      }
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
      JSONObject acl = createACL(ALL_FIELDS,
              Arrays.asList(EVERYONE, accountGuidInfo.getPublicKey()),
              ALL_FIELDS, Arrays.asList(accountGuidInfo.getPublicKey()));
      // prefix is the same for all acls so just pick one to use here
      jsonGuid.put(MetaDataTypeName.READ_WHITELIST.getPrefix(), acl);
      /* arun: You were not checking the response code below at all, which
			 * was a bug. The addGuid needs to be rolled back if the second step
			 * fails. */
      GNSResponseCode guidCode = null;
      try {
        guidCode = handler.getRemoteQuery().createRecord(guid, jsonGuid);
      } catch (ClientException ce1) {
        (guidCode = ce1.getCode()).setMessage(ce1.getMessage());
      }
      if (guidCode == null || guidCode.isExceptionOrError()) {
        // rollback name creation
        GNSResponseCode rollbackCode = handler.getRemoteQuery().deleteRecordSuppressExceptions(name);
        return new CommandResponse(
                guidCode,
                BAD_RESPONSE
                + " "
                + guidCode.getProtocolCode()
                + " "
                + guid
                + " "
                + guidCode.getMessage()
                + " "
                + (rollbackCode == null
                || !rollbackCode.isOKResult() ? "; failed to roll back "
                        + name
                        + " creation: "
                        + rollbackCode
                        + ":" + rollbackCode.getMessage()
                        : "; rolled back " + name + " creation"));
      }
      createdGUID = true;
      // else both name and guid created
      accountInfo.addGuid(guid);
      accountInfo.noteUpdate();
      updateAccountInfoNoAuthentication(accountInfo, handler, true);
      return new CommandResponse(GNSResponseCode.NO_ERROR, OK_RESPONSE
              + " " + " [created " + name + " and " + guid
              + " and updated account info successfully]");
    } catch (JSONException e) {
      return new CommandResponse(GNSResponseCode.JSON_PARSE_ERROR,
              BAD_RESPONSE + " " + JSON_PARSE_ERROR + " "
              + e.getMessage());
    } catch (ServerRuntimeException e) {
      return new CommandResponse(GNSResponseCode.UNSPECIFIED_ERROR,
              BAD_RESPONSE + " " + UNSPECIFIED_ERROR + " "
              + e.getMessage());
    } catch (ClientException ce) {
      return new CommandResponse(ce.getCode(), BAD_RESPONSE
              + " "
              + ce.getCode()
              + " "
              + ce.getMessage()
              + (createdName ? "; created "
                      + name
                      + (createdGUID ? "; created " + guid
                              + "; failed to update account info" : "")
                      : "; created neither " + name + " nor " + guid));
    }
  }

  /**
   * Add multiple guids to an account.
   *
   * @param names
   * - the list of names
   * @param publicKeys
   * - the list of public keys associated with the names
   * @param accountInfo
   * @param accountGuidInfo
   * @param handler
   * @return
   */
  public static CommandResponse addMultipleGuids(List<String> names,
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
                ALL_FIELDS,
                Arrays.asList(EVERYONE, accountGuidInfo.getPublicKey()),
                ALL_FIELDS,
                Arrays.asList(accountGuidInfo.getPublicKey()));
        // prefix is the same for all acls so just pick one to use here
        jsonGuid.put(MetaDataTypeName.READ_WHITELIST.getPrefix(), acl);
        guidInfoMap.put(guid, jsonGuid);
      }
      DelayProfiler.updateDelay("addMultipleGuidsSetup", startTime);
      accountInfo.noteUpdate();

      // first we create the HRN records as a batch
      GNSResponseCode returnCode;
      // First try to create the HRNS to insure that that name does not
      // already exist
      if (!(returnCode = handler.getRemoteQuery().createRecordBatch(
              new HashSet<>(names), hrnMap, handler))
              .isExceptionOrError()) {
        // now we update the account info
        if (updateAccountInfoNoAuthentication(accountInfo, handler, true)) {
          handler.getRemoteQuery().createRecordBatch(guids,
                  guidInfoMap, handler);
          GNSConfig.getLogger().info(DelayProfiler.getStats());
          return new CommandResponse(GNSResponseCode.NO_ERROR,
                  OK_RESPONSE);
        }
      }
      return new CommandResponse(returnCode, BAD_RESPONSE + " "
              + returnCode.getProtocolCode() + " " + names);
    } catch (JSONException e) {
      return new CommandResponse(GNSResponseCode.JSON_PARSE_ERROR,
              BAD_RESPONSE + " " + JSON_PARSE_ERROR + " "
              + e.getMessage());
    } catch (ServerRuntimeException e) {
      return new CommandResponse(GNSResponseCode.UNSPECIFIED_ERROR,
              BAD_RESPONSE + " " + UNSPECIFIED_ERROR + " "
              + e.getMessage());
    }
  }

  /**
   * Used by the batch test methods to create multiple guids. This creates
   * bunch of randomly names guids.
   *
   * @param names
   * @param accountInfo
   * @param accountGuidInfo
   * @param handler
   * @return a CommandResponse
   */
  public static CommandResponse addMultipleGuidsFaster(List<String> names,
          AccountInfo accountInfo, GuidInfo accountGuidInfo,
          ClientRequestHandlerInterface handler) {
    List<String> publicKeys = new ArrayList<>();
    for (String name : names) {
      String publicKey = "P" + name;
      publicKeys.add(publicKey);
    }
    return addMultipleGuids(names, publicKeys, accountInfo,
            accountGuidInfo, handler);
  }

  /**
   * Used by the batch test methods to create multiple guids. This creates
   * bunch of randomly names guids.
   *
   * @param accountInfo
   * @param accountGuidInfo
   * @param count
   * @param handler
   * @return
   */
  public static CommandResponse addMultipleGuidsFasterAllRandom(int count,
          AccountInfo accountInfo, GuidInfo accountGuidInfo,
          ClientRequestHandlerInterface handler) {
    List<String> names = new ArrayList<>();
    List<String> publicKeys = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      String name = "N" + RandomString.randomString(10);
      names.add(name);
      String publicKey = "P" + name;
      publicKeys.add(publicKey);
    }
    return addMultipleGuids(names, publicKeys, accountInfo,
            accountGuidInfo, handler);
  }

  /**
   * Remove a GUID. Guid should not be an account GUID.
   *
   * @param guid
   * @param handler
   * @return the command response
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws java.io.IOException
   * @throws org.json.JSONException
   */
  public static CommandResponse removeGuid(GuidInfo guid,
          ClientRequestHandlerInterface handler) throws ClientException,
          IOException, JSONException {
    return removeGuid(guid, null, false, handler);
  }

  /**
   * Remove a GUID associated with an account.
   *
   * @param accountInfo
   * @param guid
   * @param handler
   * @return status result
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws java.io.IOException
   * @throws org.json.JSONException
   */
  public static CommandResponse removeGuid(GuidInfo guid,
          AccountInfo accountInfo, ClientRequestHandlerInterface handler)
          throws ClientException, IOException, JSONException {
    return removeGuid(guid, accountInfo, false, handler);
  }

  /**
   * Remove a GUID associated with an account. If ignoreAccountGuid is true
   * we're deleting the account guid as well so we don't have to check or
   * update that info. The accountInfo parameter can be null in which case we
   * look it up using the guid.
   *
   * @param guidInfo
   * @param accountInfo
   * - can be null in which case we look it up
   * @param ignoreAccountGuid
   * @param handler
   * @return the command response
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws java.io.IOException
   * @throws org.json.JSONException
   */
  public static CommandResponse removeGuid(GuidInfo guidInfo,
          AccountInfo accountInfo, boolean ignoreAccountGuid,
          ClientRequestHandlerInterface handler) throws ClientException,
          IOException, JSONException {
    GNSConfig.getLogger().log(Level.FINE,
            "REMOVE: GUID INFO: {0} ACCOUNT INFO: {1}",
            new Object[]{guidInfo, accountInfo});
    // First make sure guid is not an account GUID
    // (unless we're sure it's not because we're deleting an account guid)
    if (!ignoreAccountGuid) {
      if (lookupAccountInfoFromGuidAnywhere(guidInfo.getGuid(), handler) != null) {
        return new CommandResponse(GNSResponseCode.BAD_GUID_ERROR,
                BAD_RESPONSE + " " + BAD_GUID + " "
                + guidInfo.getGuid() + " is an account guid");
      }
    }
    // Fill in a missing account info
    if (accountInfo == null) {
      String accountGuid = AccountAccess.lookupPrimaryGuid(
              guidInfo.getGuid(), handler, true);
      // should not happen unless records got messed up in GNS
      if (accountGuid == null) {
        return new CommandResponse(GNSResponseCode.BAD_ACCOUNT_ERROR,
                BAD_RESPONSE + " " + BAD_ACCOUNT + " "
                + guidInfo.getGuid()
                + " does not have a primary account guid");
      }
      if ((accountInfo = lookupAccountInfoFromGuidAnywhere(accountGuid, handler)) == null) {
        return new CommandResponse(GNSResponseCode.BAD_ACCOUNT_ERROR,
                BAD_RESPONSE + " " + BAD_ACCOUNT + " "
                + guidInfo.getGuid()
                + " cannot find primary account guid for "
                + accountGuid);
      }
    }
    boolean removedGroupLinks = false, deletedGUID = false, deletedName = false;
    try {
      // First remove any group links
      GroupAccess.cleanupGroupsForDelete(guidInfo.getGuid(), handler);
      removedGroupLinks = true;
      // Then remove the guid record
      if (!handler.getRemoteQuery()
              .deleteRecordSuppressExceptions(guidInfo.getGuid())
              .isExceptionOrError()) {
        deletedGUID = true;
        // remove reverse record
        handler.getRemoteQuery().deleteRecordSuppressExceptions(
                guidInfo.getName());
        deletedName = true;
        // Possibly update the account guid we are associated with to
        // tell them we are gone
        if (ignoreAccountGuid) {
          return new CommandResponse(GNSResponseCode.NO_ERROR,
                  OK_RESPONSE);
        } else {
          // update the account guid to know that we deleted the guid
          accountInfo.removeGuid(guidInfo.getGuid());
          accountInfo.noteUpdate();
          if (updateAccountInfoNoAuthentication(accountInfo, handler, true)) {
            return new CommandResponse(GNSResponseCode.NO_ERROR, OK_RESPONSE);
          } else {
            return new CommandResponse(
                    GNSResponseCode.UPDATE_ERROR, BAD_RESPONSE + " " + UPDATE_ERROR);
          }
        }
      } else {
        return new CommandResponse(GNSResponseCode.BAD_GUID_ERROR,
                BAD_RESPONSE + " " + BAD_GUID);
      }
    } catch (ClientException ce) {
      /* arun: Unclear how to roll this back or complete the rest of this
			 * operation. We need an idempotent version of this method that at
			 * least completes the operation upon a retry. */
      return new CommandResponse(ce.getCode(), BAD_RESPONSE + " "
              + ce.getMessage()
              + (removedGroupLinks ? "; removed group links" : "")
              + (deletedGUID ? "; deleted " + guidInfo.getGuid() : "")
              + (deletedName ? "; deleted " + guidInfo.getName() : "")
              + "; failed to update account info "
              + accountInfo.getGuid());
    }
  }

  /**
   * Add a new human readable name (alias) to an account.
   * <p>
   * These records will be added:<br>
   * HRN: "_GNS_GUID" -- GUID<br>
   *
   * @param accountInfo
   * @param alias
   * @param writer
   * @param signature
   * @param message
   * @param timestamp
   * @param handler
   * @return status result
   */
  public static CommandResponse addAlias(AccountInfo accountInfo,
          String alias, String writer, String signature, String message,
          Date timestamp, ClientRequestHandlerInterface handler) {
    // insure that that name does not already exist
    try {
      GNSResponseCode returnCode;
      JSONObject jsonHRN = new JSONObject();
      jsonHRN.put(HRN_GUID, accountInfo.getGuid());
      if ((returnCode = handler.getRemoteQuery().createRecord(alias,
              jsonHRN)).isExceptionOrError()) {
        // roll this back
        accountInfo.removeAlias(alias);
        return new CommandResponse(returnCode, BAD_RESPONSE + " "
                + returnCode.getProtocolCode() + " " + alias + " "
                + returnCode.getMessage());
      }
      accountInfo.addAlias(alias);
      accountInfo.noteUpdate();
      if (updateAccountInfo(accountInfo.getGuid(), accountInfo,
              writer, signature, message, timestamp, handler, true)
              .isExceptionOrError()) {
        // back out if we got an error
        handler.getRemoteQuery().deleteRecord(alias);
        return new CommandResponse(GNSResponseCode.UPDATE_ERROR,
                BAD_RESPONSE + " " + UPDATE_ERROR);
      } else {
        return new CommandResponse(GNSResponseCode.NO_ERROR,
                OK_RESPONSE);
      }
    } catch (JSONException e) {
      return new CommandResponse(GNSResponseCode.JSON_PARSE_ERROR,
              BAD_RESPONSE + " " + JSON_PARSE_ERROR + " "
              + e.getMessage());
    } catch (ClientException ce) {
      return new CommandResponse(ce.getCode(), BAD_RESPONSE + " "
              + ce.getCode() + " " + alias + " " + ce.getMessage());

    }
  }

  /**
   * Remove an alias from an account.
   *
   * @param accountInfo
   * @param alias
   * @param writer
   * @param signature
   * @param message
   * @param timestamp
   * @param handler
   * @return status result
   */
  public static CommandResponse removeAlias(AccountInfo accountInfo,
          String alias, String writer, String signature, String message,
          Date timestamp, ClientRequestHandlerInterface handler) {

    GNSConfig.getLogger().log(Level.FINE, "ALIAS: {0} ALIASES:{1}",
            new Object[]{alias, accountInfo.getAliases()});
    if (!accountInfo.containsAlias(alias)) {
      return new CommandResponse(GNSResponseCode.BAD_ALIAS_EXCEPTION,
              BAD_RESPONSE + " " + BAD_ALIAS);
    }
    // remove the NAME -- GUID record
    GNSResponseCode responseCode;
    try {
      if ((responseCode = handler.getRemoteQuery().deleteRecord(alias))
              .isExceptionOrError()) {
        return new CommandResponse(responseCode, BAD_RESPONSE + " "
                + responseCode.getProtocolCode() + " "
                + responseCode.getMessage());
      }
    } catch (ClientException ce) {
      return new CommandResponse(ce.getCode(), BAD_RESPONSE + " "
              + ce.getCode() + " " + ce.getMessage());
    }
    // Now updated the account record
    accountInfo.removeAlias(alias);
    accountInfo.noteUpdate();
    if ((responseCode = updateAccountInfo(accountInfo.getGuid(),
            accountInfo, writer, signature, message, timestamp, handler,
            true)).isExceptionOrError()) {
      return new CommandResponse(responseCode, BAD_RESPONSE + " "
              + responseCode.getProtocolCode());
    }
    return new CommandResponse(GNSResponseCode.NO_ERROR, OK_RESPONSE);
  }

  /**
   * Set the password of an account.
   *
   * @param accountInfo
   * @param password
   * @param writer
   * @param signature
   * @param message
   * @param timestamp
   * @param handler
   * @return status result
   */
  public static CommandResponse setPassword(AccountInfo accountInfo,
          String password, String writer, String signature, String message,
          Date timestamp, ClientRequestHandlerInterface handler) {
    accountInfo.setPassword(password);
    accountInfo.noteUpdate();
    if (updateAccountInfo(accountInfo.getGuid(), accountInfo,
            writer, signature, message, timestamp, handler, false)
            .isExceptionOrError()) {
      return new CommandResponse(GNSResponseCode.UPDATE_ERROR,
              BAD_RESPONSE + " " + UPDATE_ERROR);
    }
    return new CommandResponse(GNSResponseCode.NO_ERROR, OK_RESPONSE);
  }

  /**
   * Add a tag to a GUID.
   *
   * @param guidInfo
   * @param tag
   * @param writer
   * @param signature
   * @param message
   * @param timestamp
   * @param handler
   * @return status result
   */
  public static CommandResponse addTag(GuidInfo guidInfo, String tag,
          String writer, String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler) {
    guidInfo.addTag(tag);
    guidInfo.noteUpdate();
    if (updateGuidInfo(guidInfo, writer, signature, message, timestamp,
            handler).isExceptionOrError()) {
      return new CommandResponse(GNSResponseCode.UPDATE_ERROR,
              BAD_RESPONSE + " " + UPDATE_ERROR);
    }
    return new CommandResponse(GNSResponseCode.NO_ERROR, OK_RESPONSE);
  }

  /**
   * Remove a tag from a GUID.
   *
   * @param guidInfo
   * @param tag
   * @param writer
   * @param signature
   * @param message
   * @param timestamp
   * @param handler
   * @return status result
   */
  public static CommandResponse removeTag(GuidInfo guidInfo, String tag,
          String writer, String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler) {
    guidInfo.removeTag(tag);
    guidInfo.noteUpdate();
    if (updateGuidInfo(guidInfo, writer, signature, message, timestamp,
            handler).isExceptionOrError()) {
      return new CommandResponse(GNSResponseCode.UPDATE_ERROR,
              BAD_RESPONSE + " " + UPDATE_ERROR);
    }
    return new CommandResponse(GNSResponseCode.NO_ERROR, OK_RESPONSE);
  }

  private static GNSResponseCode updateAccountInfo(String guid,
          AccountInfo accountInfo, String writer, String signature,
          String message, Date timestamp,
          ClientRequestHandlerInterface handler, boolean sendToReplica) {
    try {
      GNSResponseCode response;
      if (sendToReplica) {
        // We potentially need to send the update to different replica.
        try {
          handler.getRemoteQuery().fieldUpdate(guid, ACCOUNT_INFO,
                  accountInfo.toJSONObject().toString());
          response = GNSResponseCode.NO_ERROR;
        } catch (JSONException e) {
          GNSConfig.getLogger().log(Level.SEVERE,
                  "JSON parse error with remote query:{0}", e);
          response = GNSResponseCode.JSON_PARSE_ERROR;
        } catch (ClientException | IOException e) {
          GNSConfig.getLogger().log(Level.SEVERE,
                  "Problem with remote query:{0}", e);
          response = GNSResponseCode.UNSPECIFIED_ERROR;
        }
      } else {
        // Do the update locally.
        JSONObject json = new JSONObject();
        json.put(ACCOUNT_INFO, accountInfo.toJSONObject());
        response = FieldAccess.updateUserJSON(null, guid, json, writer,
                signature, message, timestamp, handler);
      }
      return response;
    } catch (JSONException e) {
      GNSConfig.getLogger().log(Level.SEVERE,
              "Problem parsing account info:{0}", e);
      return GNSResponseCode.JSON_PARSE_ERROR;
    }
  }

  private static boolean updateAccountInfoNoAuthentication(
          AccountInfo accountInfo, ClientRequestHandlerInterface handler,
          boolean sendToReplica) {
    return !updateAccountInfo(accountInfo.getGuid(), accountInfo,
            Config.getGlobalString(GNSConfig.GNSC.INTERNAL_OP_SECRET), null, null,
            null, handler, sendToReplica)
            .isExceptionOrError();
  }

  private static GNSResponseCode updateGuidInfo(GuidInfo guidInfo,
          String writer, String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler) {
    try {
      JSONObject json = new JSONObject();
      json.put(GUID_INFO, guidInfo.toJSONObject());
      GNSResponseCode response = FieldAccess.updateUserJSON(null,
              guidInfo.getGuid(), json, writer, signature, message,
              timestamp, handler);
      return response;
    } catch (JSONException e) {
      GNSConfig.getLogger().log(Level.SEVERE,
              "Problem parsing guid info:{0}", e);
      return GNSResponseCode.JSON_PARSE_ERROR;
    }
  }

  private static boolean updateGuidInfoNoAuthentication(GuidInfo guidInfo,
          ClientRequestHandlerInterface handler) {

    return !updateGuidInfo(guidInfo,
            Config.getGlobalString(GNSConfig.GNSC.INTERNAL_OP_SECRET), null, null, null, handler)
            .isExceptionOrError();
  }

  /**
   * Returns an ACL set up to look like the JSON Object below.
   *
   * "_GNS_ACL": { "READ_WHITELIST": {|readfield|: {"MD": [readAcessor1,
   * readAcessor2,... ]}}, "WRITE_WHITELIST": {|writefield|: {"MD":
   * [writeAcessor1, writeAcessor2,... ]}}
   *
   * @param readField
   * @param readAcessors
   * @param writeField
   * @param writeAcessors
   * @return a JSONObject
   * @throws JSONException
   */
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
    String verifyCode = "000000";
    String hostPortString = "128.119.44.108:8080";
    String guid = "0FC2D9931712BCF6B7FEC5E6B09CF03483068DE";
    String emailBody = String.format(EMAIL_BODY,
            Config.getGlobalString(GNSConfig.GNSC.APPLICATION_NAME), //1$
            name, //2$
            hostPortString, //3$
            guid, //4$
            verifyCode //5$
    );
    System.out.println(emailBody);
    boolean emailOK = Email.email("GNS Account Verification", name, emailBody);

  }
}
