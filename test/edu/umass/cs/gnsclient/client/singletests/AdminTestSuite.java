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
 *
 */
package edu.umass.cs.gnsclient.client.singletests;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.utils.ThreadUtils;

import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;
import org.junit.Assert;

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Admin test for the GNS.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AdminTestSuite extends DefaultGNSTest {

  private static GNSClientCommands clientCommands;
  private static GuidEntry masterGuid;

  /**
   *
   * @throws IOException
   */
  @BeforeClass
  public static void setupBeforeClass() throws IOException {
    System.out.println("Starting client");

    clientCommands = new GNSClientCommands();
    // Make all the reads be coordinated
    clientCommands.setForceCoordinatedReads(true).setNumRetriesUponTimeout(1);
    // arun: connectivity check embedded in GNSClient constructor
    boolean connected = clientCommands instanceof GNSClient;
    if (connected) {
      System.out.println("Client created and connected to server.");
    }
    //
    int tries = 5;
    boolean accountCreated = false;

    do {
      try {
        System.out.println("Creating account guid: " + (tries - 1)
                + " attempt remaining.");
        masterGuid = GuidUtils.getGUIDKeys(globalAccountName);
        accountCreated = true;
      } catch (Exception e) {
        Utils.failWithStackTrace("Failure getting master guid");
        ThreadUtils.sleep((5 - tries) * 5000);
      }
    } while (!accountCreated && --tries > 0);
    if (accountCreated == false) {
      Utils.failWithStackTrace("Failure setting up account guid; aborting all tests.");
    }
  }

  /**
   *
   * @throws Exception
   */
  @Test
  public void test_001_CreateEntity() throws Exception {
    String alias = "testGUID" + RandomString.randomString(12);
    GuidEntry guidEntry = clientCommands.guidCreate(masterGuid, alias);
    Assert.assertNotNull(guidEntry);
    Assert.assertEquals(alias, guidEntry.getEntityName());
    clientCommands.guidRemove(masterGuid, guidEntry.getGuid());
  }
  
  /**
   *
   * @throws Exception
   */
  @Test
  public void test_04_Dump() throws Exception {
    clientCommands.dump();
  }
}
