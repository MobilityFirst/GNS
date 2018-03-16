/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsclient.client.singletests.failingtests;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.jsonassert.JSONAssert;
import edu.umass.cs.gnsclient.jsonassert.JSONCompareMode;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Basic test for the GNS using the UniversalTcpClient.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SecureCommandTest extends DefaultGNSTest {

  /**
   *
   */
  @Test
  public void test_01_SecureCreateClient() {
    try {
      client = new GNSClient();
      client.setForceCoordinatedReads(true);
    } catch (IOException e) {
      Utils.failWithStackTrace("Exception creating client: ", e);
    }
  }

  private static String accountAlias;
  private static String accountGuid = null;

  /**
   *
   */
  @Test
  public void test_02_SecureCreateAccount() {
    accountAlias = "ALIAS" + RandomString.randomString(12);
    try {
      client.execute(GNSCommand.createAccountSecure(
              accountAlias,
              "password"));
    } catch (ClientException | IOException | NoSuchAlgorithmException e) {
      Utils.failWithStackTrace("Exception while creating account: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_03_SecureCheckAccount() {
    try {
      accountGuid = client.execute(GNSCommand.lookupGUID(accountAlias)).getResultString();
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while looking up guid: ", e);
    }
    JSONObject json = null;
    if (accountGuid != null) {
      try {
        json = client.execute(GNSCommand.lookupAccountRecord(accountGuid)).getResultJSONObject();
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while looking up account record: ", e);
      }
    }
    // Also make sure the verified flag is set
    if (json != null) {
      try {
        Assert.assertTrue(json.getBoolean(GNSProtocol.ACCOUNT_RECORD_VERIFIED.toString()));
      } catch (Exception e) {
        Utils.failWithStackTrace("Exception while getting field from account record: ", e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_06_SecureRetrieveACL() {
    try {
      JSONArray expected
              = new JSONArray(new ArrayList<>(Arrays.asList(GNSProtocol.EVERYONE.toString())));
      JSONArray actual = client.execute(GNSCommand.aclGetSecure(AclAccessType.READ_WHITELIST, accountGuid,
              GNSProtocol.ENTIRE_RECORD.toString())).getResultJSONArray();
      JSONAssert.assertEquals(expected, actual, false);
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception while retrieving account record acl: ", e);
    }
  }

  private static String secondAccountAlias;
  private static String secondAccountGuid = null;

  /**
   *
   */
  @Test
  public void test_07_SecureAddSecondGuid() {
    secondAccountAlias = "SECOND" + RandomString.randomString(12);
    try {
      client.execute(GNSCommand.createAccountSecure(
              secondAccountAlias,
              "password"));
    } catch (ClientException | IOException | NoSuchAlgorithmException e) {
      Utils.failWithStackTrace("Exception while creating account: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_08_SecureGetGuid() {
    try {
      secondAccountGuid = client.execute(GNSCommand.lookupGUID(secondAccountAlias)).getResultString();
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while looking up guid: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_09_SecureAddAcl() {
    try {
      client.execute(GNSCommand.aclAddSecure(AclAccessType.READ_WHITELIST, accountGuid,
              GNSProtocol.ENTIRE_RECORD.toString(), secondAccountGuid));
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while looking up guid: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_10_SecureAddAclCheck() {
    try {
      JSONArray expected
              = new JSONArray(new ArrayList<>(Arrays.asList(GNSProtocol.EVERYONE.toString(), secondAccountGuid)));
      JSONArray actual = client.execute(GNSCommand.aclGetSecure(AclAccessType.READ_WHITELIST, accountGuid,
              GNSProtocol.ENTIRE_RECORD.toString())).getResultJSONArray();
      JSONAssert.assertEquals(expected, actual, false);
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception while retrieving account record acl: ", e);
    }
  }

  /**
   * Remove the acl.
   */
  @Test
  public void test_11_SecureRemoveAcl() {
    try {
      client.execute(GNSCommand.aclRemoveSecure(AclAccessType.READ_WHITELIST, accountGuid,
              GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.EVERYONE.toString()));
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while looking up guid: ", e);
    }
  }

  /**
   * Check to ensure the acl was removed.
   */
  @Test
  public void test_12_SecureRemoveAclCheck() {
    try {
      JSONArray expected
              = new JSONArray(new ArrayList<>(Arrays.asList(secondAccountGuid)));
      JSONArray actual = client.execute(GNSCommand.aclGetSecure(AclAccessType.READ_WHITELIST, accountGuid,
              GNSProtocol.ENTIRE_RECORD.toString())).getResultJSONArray();
      JSONAssert.assertEquals(expected, actual, true);
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception while retrieving account record acl: ", e);
    }
  }
  
  /**
   * Remove the account.
   */
  @Test
  public void test_15_SecureRead() {
    GuidEntry masterGuid = GuidUtils.getGUIDKeys(globalAccountName);
    try {
      client.execute(GNSCommand.guidCreate(masterGuid, "whatever"));
      GuidEntry testGuid = GuidUtils.lookupGuidEntryFromDatabase(client, "whatever");
      client.execute(GNSCommand.fieldUpdate(testGuid, "fred", "value"));
      JSONObject actual = client.execute(GNSCommand.readSecure(testGuid.getGuid())).getResultJSONObject(); 
      JSONAssert.assertEquals(new JSONObject().put("fred", "value"),
              actual, JSONCompareMode.STRICT);
      client.execute(GNSCommand.guidRemove(masterGuid, testGuid.getGuid()));
    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception while removing account record: ", e);
    }
  }
  
  
  /**
   * Remove the account.
   */
  @Test
  public void test_19_SecureRemoveAccount() {
    try {
      client.execute(GNSCommand.accountGuidRemoveSecure(secondAccountAlias));
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing account record: ", e);
    }
  }

  /**
   * Remove the account.
   */
  @Test
  public void test_20_SecureRemoveAccount() {
    try {
      client.execute(GNSCommand.accountGuidRemoveSecure(accountAlias));
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing account record: ", e);
    }
  }

  /**
   * Check to make sure account was removed.
   */
  @Test
  public void test_21_SecureRemoveAccountCheck() {

    try {
      client.execute(GNSCommand.lookupGUID(accountAlias)).getResultString();
      Utils.failWithStackTrace("Should have throw a client "
              + "exception while looking the guid for " + accountAlias);
    } catch (ClientException | IOException e) {
    }
  }

  /**
   * Remove the account.
   */
  @Test
  public void test_22_SecureRemoveAccountAgain() {
    try {
      client.execute(GNSCommand.accountGuidRemoveSecure(accountAlias));
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing account record: ", e);
    }
  }
}
