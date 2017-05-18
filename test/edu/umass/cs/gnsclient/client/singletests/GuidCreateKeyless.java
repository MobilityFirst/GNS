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
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.MetaDataTypeName;
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
public class GuidCreateKeyless extends DefaultGNSTest {

  private static GuidEntry masterGuid;

  /**
   *
   */
  public GuidCreateKeyless() {
    try {
      masterGuid = GuidUtils.getGUIDKeys(globalAccountName);
    } catch (Exception e) {
      Utils.failWithStackTrace("Exception when we were not expecting it: " + e);
    }
  }

  private static String guidName = "testGUID" + RandomString.randomString(12);

  /**
   *
   */
  @Test
  public void test_10_Create() {
    try {
      client.execute(GNSCommand.guidCreateKeyless(masterGuid, guidName));
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while creating testGuid: " + e);
    }
  }

  
  // Add this back when mutual auth commands are working again - see MOB-1165
  // Note that the only way to change an ACL of a guid is through a command that requires
  // public key of the guid, but since we're working with keyless guids the only way to change
  // the ACL is through a mutual auth command like below. Or we add an acl command that
  // allows another guid to change the acl of a different guid if it has access, 
  // but we currently don't support that.
//  /**
//   *
//   */
//  @Test
//  public void test_20_Acl_Update() {
//    String guid = null;
//    try {
//      guid = client.execute(GNSCommand.lookupGUID(guidName)).getResultString();
//    } catch (ClientException | IOException e) {
//      Utils.failWithStackTrace("Exception while doing lookup: " + e);
//    }
//    try {
//      client.execute(GNSCommand.aclRemoveSecure(AclAccessType.READ_WHITELIST, guid,
//              GNSProtocol.ENTIRE_RECORD.toString(), masterGuid.getGuid())).getResultString();
//      Utils.failWithStackTrace("aclRemoveSecure should have throw an exception");
//    } catch (ClientException e) {
//      // normal result
//    } catch (IOException e) {
//      Utils.failWithStackTrace("Exception while doing laclRemoveSecure: " + e);
//    }
//    try {
//      client.execute(GNSCommand.aclRemoveSecure(AclAccessType.WRITE_WHITELIST, guid,
//              GNSProtocol.ENTIRE_RECORD.toString(), masterGuid.getGuid())).getResultString();
//      Utils.failWithStackTrace("aclRemoveSecure should have throw an exception");
//    } catch (ClientException e) {
//      // normal result
//    } catch (IOException e) {
//      Utils.failWithStackTrace("Exception while doing laclRemoveSecure: " + e);
//    }
//  }

  /**
   *
   */
  @Test
  public void test_30_Remove() {
    String guid = null;
    try {
      guid = client.execute(GNSCommand.lookupGUID(guidName)).getResultString();
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while doing lookup: " + e);
    }

    try {
      System.out.println("record is " + client.execute(GNSCommand.lookupGUIDRecord(guid)).getResultString());
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while doing lookup: " + e);
    }

    try {
      client.execute(GNSCommand.guidRemove(masterGuid, guid));
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while removing testGuid (" + guid + "): " + e);
    }
    try {
      client.execute(GNSCommand.lookupGUIDRecord(guidName));
      Utils.failWithStackTrace("Lookup testGuid should have throw an exception.");
    } catch (ClientException e) {
      // expected result
    } catch (IOException e) {
      Utils.failWithStackTrace("Exception while doing Lookup testGuid: " + e);
    }
  }
}
