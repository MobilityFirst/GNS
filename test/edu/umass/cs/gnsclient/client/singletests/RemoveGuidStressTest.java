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

import edu.umass.cs.utils.Utils;
import java.io.IOException;

import java.security.NoSuchAlgorithmException;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Comprehensive functionality test for the GNS.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RemoveGuidStressTest {

  private static String ACCOUNT_ALIAS = "support@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static GNSClientCommands client;
  private static GuidEntry masterGuid;

  private static final int cnt = 100;
  private static final GuidEntry[] guids = new GuidEntry[cnt];

  /**
   *
   */
  public RemoveGuidStressTest() {
    if (client == null) {
      try {
        client = new GNSClientCommands();
        client.setForceCoordinatedReads(true);
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception creating client: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_10_CreateAccountGuid() {
    try {
      masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_20_RemoveGuidUsingAccountCreateGuids() {
    String testGuidName = "testGUID" + RandomString.randomString(10);
    for (int i = 0; i < cnt; i++) {
      try {
        guids[i] = client.guidCreate(masterGuid, testGuidName + "-" + i);
      } catch (NoSuchAlgorithmException | ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while creating testGuid: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_30_RemoveGuidUsingAccountRemove() {
    for (int i = 0; i < cnt; i++) {
      try {
        client.guidRemove(masterGuid, guids[i].getGuid());
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while removing testGuid (" + guids[i].toString() + "): " + e);
      }
      try {
        client.lookupGuidRecord(guids[i].getGuid());
        Utils.failWithStackTrace("Lookup testGuid should have throw an exception.");
      } catch (ClientException e) {

      } catch (IOException e) {
        Utils.failWithStackTrace("Exception while doing Lookup testGuid: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_40_RemoveGuidCreateGuids() {
    String testGuidName = "testGUID" + RandomString.randomString(10);
    for (int i = 0; i < cnt; i++) {
      try {
        guids[i] = client.guidCreate(masterGuid, testGuidName + "-" + i);
      } catch (NoSuchAlgorithmException | ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while creating testGuid: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_50_RemoveGuidRemove() {
    for (int i = 0; i < cnt; i++) {
      try {
        client.guidRemove(guids[i]);
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while removing testGuid: " + e);
      }
      try {
        client.lookupGuidRecord(guids[i].getGuid());
        Utils.failWithStackTrace("Lookup testGuid should have throw an exception.");
      } catch (ClientException e) {

      } catch (IOException e) {
        Utils.failWithStackTrace("Exception while doing Lookup testGuid: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_60_RemoveAccountGuidCreateGuids() {
    String testGuidName = "testGUID" + RandomString.randomString(10);
    for (int i = 0; i < cnt; i++) {
      try {
        guids[i] = client.accountGuidCreate(testGuidName + "-" + i, PASSWORD);
      } catch (Exception e) {
        Utils.failWithStackTrace("Exception while creating testGuid: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_70_RemoveAccountGuidRemove() {
    for (int i = 0; i < cnt; i++) {
      try {
        client.accountGuidRemove(guids[i]);
      } catch (Exception e) {
        Utils.failWithStackTrace("Exception while removing testGuid (" + guids[i].toString() + "): " + e);
      }
      try {
        client.lookupGuidRecord(guids[i].getGuid());
        Utils.failWithStackTrace("Lookup testGuid should have throw an exception.");
      } catch (ClientException e) {

      } catch (IOException e) {
        Utils.failWithStackTrace("Exception while doing Lookup testGuid: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_80_RemoveAccount() {
    try {
      client.accountGuidRemove(masterGuid);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception while removing testGuid: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_90_CheckForRemoval() {
    try {
      client.lookupGuid(ACCOUNT_ALIAS);
      Utils.failWithStackTrace("lookupGuid for " + ACCOUNT_ALIAS + " should have throw an exception.");
    } catch (ClientException e) {

    } catch (IOException e) {
      Utils.failWithStackTrace("Exception while lookupAccountRecord for " + ACCOUNT_ALIAS + " :" + e);
    }
  }
}
