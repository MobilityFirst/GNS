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
import edu.umass.cs.gnsclient.client.util.GUIDUtilsHTTPClient;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
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
public class RemoveGuidStressTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands;
  private static GuidEntry masterGuid;

  private static final int NUMBER_TO_CREATE = 100;
  private static final GuidEntry[] GUIDS = new GuidEntry[NUMBER_TO_CREATE];
  private static final String PASSWORD = "removalPassword";

  /**
   *
   */
  public RemoveGuidStressTest() {
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
  public void test_10_LookupAccountGuid() {
    try {
      masterGuid = GUIDUtilsHTTPClient.getGUIDKeys(globalAccountName);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_20_RemoveGuidUsingAccountCreateGuids() {
    String testGuidName = "testGUID" + RandomString.randomString(12);
    for (int i = 0; i < NUMBER_TO_CREATE; i++) {
      try {
        GUIDS[i] = clientCommands.guidCreate(masterGuid, testGuidName + "-" + i);
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while creating testGuid: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_30_RemoveGuidUsingAccountRemove() {
    for (int i = 0; i < NUMBER_TO_CREATE; i++) {
      try {
        clientCommands.guidRemove(masterGuid, GUIDS[i].getGuid());
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while removing testGuid (" + GUIDS[i].toString() + "): " + e);
      }
      try {
        clientCommands.lookupGuidRecord(GUIDS[i].getGuid());
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
    String testGuidName = "testGUID" + RandomString.randomString(12);
    for (int i = 0; i < NUMBER_TO_CREATE; i++) {
      try {
        GUIDS[i] = clientCommands.guidCreate(masterGuid, testGuidName + "-" + i);
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while creating testGuid: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_50_RemoveGuidRemove() {
    for (int i = 0; i < NUMBER_TO_CREATE; i++) {
      try {
        clientCommands.guidRemove(GUIDS[i]);
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while removing testGuid: " + e);
      }
      try {
        clientCommands.lookupGuidRecord(GUIDS[i].getGuid());
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
    String testGuidName = "testGUID" + RandomString.randomString(12);
    for (int i = 0; i < NUMBER_TO_CREATE; i++) {
      try {
        GUIDS[i] = clientCommands.accountGuidCreate(testGuidName + "-" + i, PASSWORD);
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while creating testGuid: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_70_RemoveAccountGuidRemove() {
    for (int i = 0; i < NUMBER_TO_CREATE; i++) {
      try {
        clientCommands.accountGuidRemove(GUIDS[i]);
      } catch (ClientException | IOException e) {
        Utils.failWithStackTrace("Exception while removing testGuid (" + GUIDS[i].toString() + "): " + e);
      }
      try {
        clientCommands.lookupGuidRecord(GUIDS[i].getGuid());
        Utils.failWithStackTrace("Lookup testGuid should have throw an exception.");
      } catch (ClientException e) {

      } catch (IOException e) {
        Utils.failWithStackTrace("Exception while doing Lookup testGuid: " + e);
      }
    }
  }
  
}
