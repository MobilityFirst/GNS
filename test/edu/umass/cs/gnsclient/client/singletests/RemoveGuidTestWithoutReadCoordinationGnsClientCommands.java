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
public class RemoveGuidTestWithoutReadCoordinationGnsClientCommands extends DefaultGNSTest {

  /**
   *
   */
  protected static GNSClientCommands clientCommands;
  private static GuidEntry masterGuid;
  private static GuidEntry guidToDeleteEntry;

  /**
   *
   */
  public RemoveGuidTestWithoutReadCoordinationGnsClientCommands() {
    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception while creating client: " + e);
      }
      clientCommands.setForceCoordinatedReads(false);
    }
  }

  /**
   *
   */
  @Test
  public void test_01_testCreateAccountGuid() {
    try {
      masterGuid = GUIDUtilsHTTPClient.getGUIDKeys(globalAccountName);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception while creating account guid: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_210_GuidCreate() {
    String deleteName = "deleteMe" + RandomString.randomString(12);
    try {
      try {
        clientCommands.lookupGuid(deleteName);
        Utils.failWithStackTrace(deleteName + " entity should not exist");
      } catch (ClientException e) {
      }
      guidToDeleteEntry = clientCommands.guidCreate(masterGuid, deleteName);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while creating guids: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_212_GuidCreateCheck() {
    try {
      clientCommands.lookupGuidRecord(guidToDeleteEntry.getGuid());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while doing Lookup testGuid: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_214_GroupRemoveGuid() {
    try {
      clientCommands.guidRemove(masterGuid, guidToDeleteEntry.getGuid());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing testGuid: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_216_GroupRemoveGuidCheck() {
    try {
      clientCommands.lookupGuidRecord(guidToDeleteEntry.getGuid());
      Utils.failWithStackTrace("Lookup testGuid should have throw an exception.");
    } catch (ClientException e) {

    } catch (IOException e) {
      Utils.failWithStackTrace("Exception while doing Lookup testGuid: " + e);
    }

  }
}
