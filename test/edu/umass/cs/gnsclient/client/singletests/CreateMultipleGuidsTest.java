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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Comprehensive functionality test for the GNS.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CreateMultipleGuidsTest {

  private static final String ACCOUNT_ALIAS = "admin@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static GNSClientCommands client;
  private static GuidEntry masterGuid;

  private static final String fieldName = "_MultipleGuidsTestField_";
  private List<GuidEntry> members = new ArrayList<>();

  /**
   *
   */
  public CreateMultipleGuidsTest() {
    if (client == null) {
       try {
        client = new GNSClientCommands();
        client.setForceCoordinatedReads(true);
      } catch (IOException e) {
        fail("Exception creating client: " + e);
      }
      try {
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
      } catch (Exception e) {
        fail("Exception while creating account guid: " + e);
      }
    }
  }

  /**
   * Create the guids.
   */
  @Test
  public void test_01_SetupGuids() {
    try {
      for (int cnt = 0; cnt < 10; cnt++) {
        GuidEntry testEntry = client.guidCreate(masterGuid, "queryTest-" + RandomString.randomString(6));
        members.add(testEntry);
        // make unique name based on the guid
        client.fieldUpdate(testEntry, fieldName, "value for " + testEntry.getEntityName());
      }
    } catch (Exception e) {
      fail("Exception while trying to create the guids: " + e);
    }
  }
  
  /**
   * Read back the values.
   */
  @Test
  public void test_02_TestGuids() {
    try {
      for (GuidEntry guidEntry : members) {
        String actual = client.fieldRead(guidEntry, fieldName);
        String expected = "value for " + guidEntry.getEntityName();
        assertEquals(expected, actual);
      }
    } catch (Exception e) {
      fail("Exception while trying to read values: " + e);
    }
  }
}
