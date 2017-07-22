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
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.GNSProtocol;
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
 * This test insures that a simple acl add gives field access to another guid.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AclAllFieldsSuperuser extends DefaultGNSTest {

  private static GNSClientCommands clientCommands = null;
  private static GuidEntry masterGuid;

  /**
   *
   */
  public AclAllFieldsSuperuser() {
    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
        clientCommands.setForceCoordinatedReads(true).setNumRetriesUponTimeout(1);
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception creating client: ", e);
      }
    }
  }

  private static GuidEntry barneyEntry;

  /**
   *
   */
  @Test
  public void test_100_LookupMasterGuid() {
    try {
      masterGuid = GuidUtils.getGUIDKeys(globalAccountName);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception while creating guid: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_142_ACLCreateAnotherGuid() {
    try {
      String barneyName = "barney" + RandomString.randomString(12);
      try {
        clientCommands.lookupGuid(barneyName);
        Utils.failWithStackTrace(barneyName + " entity should not exist");
      } catch (ClientException e) {
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception looking up Barney: ", e);
      }
      barneyEntry = clientCommands.guidCreate(masterGuid, barneyName);
      try {
        clientCommands.lookupGuid(barneyName);
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception looking up Barney: ", e);
      }
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it in ACLPartTwo: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_143_ACLCreateFields() {
    try {
      // remove default read access for this test
      clientCommands.fieldUpdate(barneyEntry.getGuid(), "cell", "413-555-1234", barneyEntry);
      clientCommands.fieldUpdate(barneyEntry.getGuid(), "address", "100 Main Street", barneyEntry);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it in ACLPartTwo: ", e);
    }
  }

  private static GuidEntry superuserEntry;

  /**
   *
   */
  @Test
  public void test_144_ACLCreateSuperUser() {
    String superUserName = "superuser" + RandomString.randomString(12);
    try {
      try {
        clientCommands.lookupGuid(superUserName);
        Utils.failWithStackTrace(superUserName + " entity should not exist");
      } catch (ClientException e) {
      }
      superuserEntry = clientCommands.guidCreate(masterGuid, superUserName);
      try {
        Assert.assertEquals(superuserEntry.getGuid(), clientCommands.lookupGuid(superUserName));
      } catch (ClientException e) {
      }
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it in ACLALLFields: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_145_ACLAddALLFields() {
    try {
      // let superuser read any of barney's fields
      clientCommands.aclAdd(AclAccessType.READ_WHITELIST, barneyEntry,
              GNSProtocol.ENTIRE_RECORD.toString(), superuserEntry.getGuid());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it in ACLALLFields: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_146_ACLTestAllFieldsSuperuser() {
    try {
      Assert.assertEquals("413-555-1234",
              clientCommands.fieldRead(barneyEntry.getGuid(), "cell", superuserEntry));
      Assert.assertEquals("100 Main Street",
              clientCommands.fieldRead(barneyEntry.getGuid(), "address", superuserEntry));

    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception when we were not expecting it in ACLALLFields: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_147_ACLTestAllFieldsSuperuserCleanup() {
    try {
      clientCommands.guidRemove(masterGuid, superuserEntry.getGuid());
      clientCommands.guidRemove(masterGuid, superuserEntry.getGuid());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception during cleanup: " + e);
    }
  }

}
