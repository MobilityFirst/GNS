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
import edu.umass.cs.gnscommon.exceptions.client.ClientException;

import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Test accountGuidRemoveWithPassword.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RemoveAccountWithPasswordTest extends DefaultGNSTest {

  private static final String ACCOUNT_TO_REMOVE = "remove" + RandomString.randomString(12) + "@gns.name";
  private static final String PASSWORD = "removalPassword";
  private static GNSClientCommands clientCommands;
  private static GuidEntry accountToRemoveGuid;

  /**
   *
   */
  public RemoveAccountWithPasswordTest() {
    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
        clientCommands.setForceCoordinatedReads(true).setNumRetriesUponTimeout(1);
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception creating client in RemoveAccountWithPasswordTest: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_35_RemoveAccountWithPasswordCreateAccount() {
    try {
      accountToRemoveGuid = GuidUtils.lookupOrCreateAccountGuid(clientCommands,
              ACCOUNT_TO_REMOVE, PASSWORD, true);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception creating account in RemoveAccountWithPasswordTest: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_36_RemoveAccountWithPasswordCheckAccount() {
    try {
      // this should be using the guid
      clientCommands.lookupAccountRecord(accountToRemoveGuid.getGuid());
    } catch (ClientException e) {
      Utils.failWithStackTrace("lookupAccountRecord for " + ACCOUNT_TO_REMOVE + " failed.");
    } catch (IOException e) {
      Utils.failWithStackTrace("Exception while lookupAccountRecord for " + ACCOUNT_TO_REMOVE + " :" + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_37_RemoveAccountWithPasswordRemoveAccount() {
    try {
      clientCommands.accountGuidRemoveWithPassword(ACCOUNT_TO_REMOVE, PASSWORD);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing masterGuid in RemoveAccountWithPasswordTest: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_38_RemoveAccountWithPasswordCheckAccountAfterRemove() {
    try {
      clientCommands.lookupGuid(ACCOUNT_TO_REMOVE);
      Utils.failWithStackTrace("lookupGuid for " + ACCOUNT_TO_REMOVE + " should have throw an exception.");
    } catch (ClientException e) {

    } catch (IOException e) {
      Utils.failWithStackTrace("Exception in RemoveAccountWithPasswordTest while lookupAccountRecord for " + ACCOUNT_TO_REMOVE + " :" + e);
    }
  }
}
