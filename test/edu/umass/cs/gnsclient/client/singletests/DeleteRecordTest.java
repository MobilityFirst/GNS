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
package edu.umass.cs.gnsclient.client.singletests;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;

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
public class DeleteRecordTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands;
  private static final String TEST_ACCOUNT_ALIAS = "support-"
          + RandomString.randomString(24) + "@gns.name";
  private static final String TEST_ACCOUNT_PASSWORD = "password";
  private static GuidEntry testGuid;

  /**
   *
   */
  public DeleteRecordTest() {
    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
        clientCommands.setForceCoordinatedReads(true);
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception creating client: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_01_CreateAccount() {
    try {
      testGuid = GuidUtils.lookupOrCreateAccountGuid(clientCommands,
              TEST_ACCOUNT_ALIAS, TEST_ACCOUNT_PASSWORD, true);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }

  }

  /**
   *
   */
  @Test
  public void test_02_CheckAccount() {
    String guidString = null;
    try {
      guidString = clientCommands.lookupGuid(TEST_ACCOUNT_ALIAS);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception while looking up guid: " + e);
    }
    JSONObject json = null;
    if (guidString != null) {
      try {
        json = clientCommands.lookupAccountRecord(guidString);
      } catch (Exception e) {
        Utils.failWithStackTrace("Exception while looking up account record: " + e);
      }
    }
    if (json != null) {
      try {
        Assert.assertTrue(json.getBoolean(GNSProtocol.ACCOUNT_RECORD_VERIFIED.toString()));
      } catch (Exception e) {
        Utils.failWithStackTrace("Exception while getting field from account record: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_04_DeleteHRN() {
    try {
      clientCommands.deleteRecord(TEST_ACCOUNT_ALIAS);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing test account guid: " + e);
    }
  }

  @Test
  public void test_05_CheckHRN() {
    try {
      clientCommands.lookupGuid(TEST_ACCOUNT_ALIAS);
      Utils.failWithStackTrace("Should not have found the alias " + TEST_ACCOUNT_ALIAS);
    } catch (Exception e) {
      System.out.println("This is expected: Caught " + e.getMessage());
    }
  }

  @Test
  public void test_06_DeleteGuid() {
    try {
      clientCommands.deleteRecord(testGuid.getGuid());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing test account guid: " + e);
    }
  }

  @Test
  public void test_07_CheckGuid() {
    try {
      clientCommands.lookupAccountRecord(testGuid.getGuid());
      Utils.failWithStackTrace("Should not have found the guid " + testGuid.getGuid());
    } catch (Exception e) {
      System.out.println("This is expected: Caught " + e.getMessage());
    }
  }

  // no need for additional cleanup because we deleted the record we created
}
