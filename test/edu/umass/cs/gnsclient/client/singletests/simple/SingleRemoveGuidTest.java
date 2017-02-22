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
package edu.umass.cs.gnsclient.client.singletests.simple;

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
 * Test removes.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SingleRemoveGuidTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands;
  private static GuidEntry masterGuid;

  /**
   *
   */
  public SingleRemoveGuidTest() {
    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
        clientCommands.setForceCoordinatedReads(true);
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
  public void test_01_RemoveGuidUsingAccount() {
    String testGuidName = "testGUID" + RandomString.randomString(12);
    GuidEntry testGuid = null;
    try {
      testGuid = clientCommands.guidCreate(masterGuid, testGuidName);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while creating testGuid: " + e);
    }
    if (testGuid != null) {
      try {
        clientCommands.guidRemove(masterGuid, testGuid.getGuid());
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
  }

}
