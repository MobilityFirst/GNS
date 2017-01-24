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
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Comprehensive functionality test for the GNS.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CreateMultipleGuidsTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands;
  private static GuidEntry masterGuid;

  private static final String FIELD_NAME = "_MultipleGuidsTestField_";
  private List<GuidEntry> members = new ArrayList<>();

  /**
   *
   */
  public CreateMultipleGuidsTest() {
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
        Utils.failWithStackTrace("Exception while creating account guid: " + e);
      }
    }
  }

  /**
   * Create the guids.
   */
  @Test
  public void test_01_SetupGuids() {
    try {
      for (int cnt = 0; cnt < 50; cnt++) {
        GuidEntry testEntry = clientCommands.guidCreate(
                masterGuid, "guid" + RandomString.randomString(12));
        members.add(testEntry);
        // make unique name based on the guid
        clientCommands.fieldUpdate(testEntry, FIELD_NAME,
                "value for " + testEntry.getEntityName());
      }
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while trying to create the guids: " + e);
    }
  }

  /**
   * Read back the values.
   */
  @Test
  public void test_02_TestGuids() {
    try {
      for (GuidEntry guidEntry : members) {
        String actual = clientCommands.fieldRead(guidEntry, FIELD_NAME);
        String expected = "value for " + guidEntry.getEntityName();
        Assert.assertEquals(expected, actual);
      }
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while trying to read values: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_03_Cleanup() {
    try {
      for (GuidEntry guidEntry : members) {
        clientCommands.guidRemove(masterGuid, guidEntry.getGuid());
      }
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing test account guid: " + e);
    }
  }
}
