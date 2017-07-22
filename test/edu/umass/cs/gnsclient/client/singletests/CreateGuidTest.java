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

import org.junit.Assert;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Basic test for the GNS using the UniversalTcpClient.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CreateGuidTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands;
  private static GuidEntry masterGuid;

  /**
   *
   */
  public CreateGuidTest() {

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
  public void test_01_CreateEntity() {
    String alias = "testGUID" + RandomString.randomString(12);
    GuidEntry guidEntry = null;
    try {
      guidEntry = clientCommands.guidCreate(masterGuid, alias);
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while creating guid: " + e);
    }
    Assert.assertNotNull(guidEntry);
    Assert.assertEquals(alias, guidEntry.getEntityName());
    try {
      clientCommands.guidRemove(masterGuid, guidEntry.getGuid());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while creating guid: " + e);
    }
  }

}
