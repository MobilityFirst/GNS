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
import java.io.IOException;
import static org.junit.Assert.*;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * This test
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AclAllFieldsSuperuser {

  private static final String ACCOUNT_ALIAS = "acltest@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static GNSClientCommands client = null;
  private static GuidEntry masterGuid;

  /**
   *
   */
  public AclAllFieldsSuperuser() {
    if (client == null) {
      try {
        client = new GNSClientCommands();
        client.setForceCoordinatedReads(true);
      } catch (IOException e) {
        failWithStackTrace("Exception creating client: ", e);
      }
    }
  }

  private static GuidEntry barneyEntry;

  /**
   *
   */
  @Test
  public void test_100_CreateGuid() {
    try {
      masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
    } catch (Exception e) {
      failWithStackTrace("Exception while creating guid: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_142_ACLCreateAnotherGuid() {
    try {
      String barneyName = "barney" + RandomString.randomString(6);
      try {
        client.lookupGuid(barneyName);
        failWithStackTrace(barneyName + " entity should not exist");
      } catch (ClientException e) {
      } catch (IOException e) {
        failWithStackTrace("Exception looking up Barney: ", e);
      }
      barneyEntry = client.guidCreate(masterGuid, barneyName);
      try {
        client.lookupGuid(barneyName);
      } catch (IOException | ClientException e) {
        failWithStackTrace("Exception looking up Barney: ", e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLPartTwo: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_143_ACLCreateFields() {
    try {
      // remove default read access for this test
      client.fieldUpdate(barneyEntry.getGuid(), "cell", "413-555-1234", barneyEntry);
      client.fieldUpdate(barneyEntry.getGuid(), "address", "100 Main Street", barneyEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLPartTwo: ", e);
      e.printStackTrace();
    }
  }

  private static GuidEntry superuserEntry;

  /**
   *
   */
  @Test
  public void test_144_ACLCreateSuperUser() {
    String superUserName = "superuser" + RandomString.randomString(6);
    try {
      try {
        client.lookupGuid(superUserName);
        failWithStackTrace(superUserName + " entity should not exist");
      } catch (ClientException e) {
      }

      superuserEntry = client.guidCreate(masterGuid, superUserName);
      try {
        assertEquals(superuserEntry.getGuid(), client.lookupGuid(superUserName));
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLALLFields: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_145_ACLAddALLFields() {
    try {
      // let superuser read any of barney's fields
      client.aclAdd(AclAccessType.READ_WHITELIST, barneyEntry,
              GNSProtocol.ENTIRE_RECORD.toString(), superuserEntry.getGuid());
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLALLFields: ", e);
    }
  }

  /**
   *
   */
  @Test
  public void test_146_ACLTestAllFieldsSuperuser() {
    try {
      assertEquals("413-555-1234",
              client.fieldRead(barneyEntry.getGuid(), "cell", superuserEntry));
      assertEquals("100 Main Street",
              client.fieldRead(barneyEntry.getGuid(), "address", superuserEntry));

    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLALLFields: ", e);
    }
  }

  private static final void failWithStackTrace(String message, Exception... e) {
    if (e != null && e.length > 0) {
      e[0].printStackTrace();
    }
    org.junit.Assert.fail(message);
  }
}
