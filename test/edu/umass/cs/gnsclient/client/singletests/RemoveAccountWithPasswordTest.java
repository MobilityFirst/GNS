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

import java.io.IOException;

import static org.junit.Assert.*;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Comprehensive functionality test for the GNS.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RemoveAccountWithPasswordTest {

  private static String ACCOUNT_TO_REMOVE_WITH_PASSWORD = "remove@gns.name";
  private static final String REMOVE_ACCOUNT_PASSWORD = "removalPassword";
  private static GNSClientCommands client;
  private static GuidEntry accountToRemoveGuid;

  /**
   *
   */
  public RemoveAccountWithPasswordTest() {
    if (client == null) {
      try {
        client = new GNSClientCommands();
        client.setForceCoordinatedReads(true);
      } catch (IOException e) {
        fail("Exception creating client in RemoveAccountWithPasswordTest: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_35_RemoveAccountWithPasswordCreateAccount() {
    try {
      accountToRemoveGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_TO_REMOVE_WITH_PASSWORD, REMOVE_ACCOUNT_PASSWORD, true);
    } catch (Exception e) {
      fail("Exception creating account in RemoveAccountWithPasswordTest: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_36_RemoveAccountWithPasswordCheckAccount() {
    try {
      // this should be using the guid
      client.lookupAccountRecord(accountToRemoveGuid.getGuid());
    } catch (ClientException e) {
      fail("lookupAccountRecord for " + ACCOUNT_TO_REMOVE_WITH_PASSWORD + " failed.");
    } catch (IOException e) {
      fail("Exception while lookupAccountRecord for " + ACCOUNT_TO_REMOVE_WITH_PASSWORD + " :" + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_37_RemoveAccountWithPasswordRemoveAccount() {
    try {
      client.accountGuidRemoveWithPassword(ACCOUNT_TO_REMOVE_WITH_PASSWORD, REMOVE_ACCOUNT_PASSWORD);
    } catch (Exception e) {
      fail("Exception while removing masterGuid in RemoveAccountWithPasswordTest: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_38_RemoveAccountWithPasswordCheckAccountAfterRemove() {
    try {
      client.lookupGuid(ACCOUNT_TO_REMOVE_WITH_PASSWORD);
      fail("lookupGuid for " + ACCOUNT_TO_REMOVE_WITH_PASSWORD + " should have throw an exception.");
    } catch (ClientException e) {

    } catch (IOException e) {
      fail("Exception in RemoveAccountWithPasswordTest while lookupAccountRecord for " + ACCOUNT_TO_REMOVE_WITH_PASSWORD + " :" + e);
    }
  }
}
