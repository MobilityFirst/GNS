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
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;

import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Comprehensive functionality test for the GNS.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RemoveGuidTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands;
  private static GuidEntry masterGuid;

  /**
   *
   */
  public RemoveGuidTest() {
    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
        clientCommands.setForceCoordinatedReads(true).setNumRetriesUponTimeout(1);
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception creating client: " + e);
      }
      try {
        masterGuid = GuidUtils.getGUIDKeys(globalAccountName);
      } catch (Exception e) {
        Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_10_RemoveGuidUsingAccount() {
    String guidName = "testGUID" + RandomString.randomString(12);
    GuidEntry guid = null;
    try {
      guid = clientCommands.guidCreate(masterGuid, guidName);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while creating testGuid: " + e);
    }

    if (guid != null) {
      System.out.println("testGuid is " + guid.toString());

      try {
        clientCommands.guidRemove(masterGuid, guid.getGuid());
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while removing testGuid (" + guid.toString() + "): " + e);
      }
      try {
        clientCommands.lookupGuidRecord(guid.getGuid());
        Utils.failWithStackTrace("Lookup testGuid should have throw an exception.");
      } catch (ClientException e) {

      } catch (IOException e) {
        Utils.failWithStackTrace("Exception while doing Lookup testGuid: " + e);
      }
    }
  }

  private static final String testGuidName = "testGUID" + RandomString.randomString(12);
  private static GuidEntry testGuid = null;

  /**
   *
   */
  @Test
  public void test_20_RemoveGuid() {
    try {
      testGuid = clientCommands.guidCreate(masterGuid, testGuidName);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while creating testGuid: " + e);
    }
    try {
      clientCommands.guidRemove(testGuid);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing testGuid: " + e);
    }
    try {
      clientCommands.lookupGuidRecord(testGuid.getGuid());
      Utils.failWithStackTrace("Lookup testGuid should have throw an exception.");
    } catch (ClientException e) {

    } catch (IOException e) {
      Utils.failWithStackTrace("Exception while doing Lookup testGuid: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_30_RemoveGuidAgain() {
    try {
      clientCommands.guidRemove(testGuid);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing testGuid: " + e);
    }
  }

  private static final String ACCOUNT_TO_REMOVE = "remove" + RandomString.randomString(12) + "@gns.name";
  private static final String PASSWORD = "removalPassword";
  private static GuidEntry accountToRemoveGuid;

  /**
   *
   */
  @Test
  public void test_80_RemoveAccountCreateAccount() {
    try {
      accountToRemoveGuid = GuidUtils.lookupOrCreateAccountGuid(clientCommands,
              ACCOUNT_TO_REMOVE, PASSWORD, true);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception creating account in: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_82_RemoveAccount() {
    try {
      clientCommands.accountGuidRemove(accountToRemoveGuid);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing account: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_84_CheckForRemoval() {
    try {
      clientCommands.lookupGuid(ACCOUNT_TO_REMOVE);
      Utils.failWithStackTrace("lookupGuid for " + ACCOUNT_TO_REMOVE + " should have throw an exception.");
    } catch (ClientException e) {
      // normal result
    } catch (IOException e) {
      Utils.failWithStackTrace("Exception while lookupAccountRecord for " + ACCOUNT_TO_REMOVE + " :" + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_86_RemoveAccountAgain() {
    try {
      clientCommands.accountGuidRemove(accountToRemoveGuid);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing account again: " + e);
    }
  }
}
