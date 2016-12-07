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

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.utils.RandomString;
import java.io.IOException;

import org.json.JSONObject;

import static org.junit.Assert.*;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Basic test for the GNS using the UniversalTcpClient.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CreateAccountSecureTest {
  
  private static GNSClient client;

  /**
   *
   */
  public CreateAccountSecureTest() {
  }

  /**
   *
   */
  @Test
  public void test_01_CreateClient() {
    try {
      client = new GNSClient();
      //client.setForceCoordinatedReads(true);
    } catch (IOException e) {
      fail("Exception creating client: " + e);
    }
  }

  private static String accountAlias;
  /**
   *
   */
  @Test
  public void test_02_CreateAccount() {
    accountAlias = "ALIAS" + RandomString.randomString(12);
    try {
      client.execute(GNSCommand.accountGuidCreateSecure(client.getGNSProvider(), 
              accountAlias,
              "password"));
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_03_CheckAccount() {
    String guidString = null;
    try {
      guidString = client.execute(GNSCommand.lookupGUID(accountAlias)).getResultString();
    } catch (Exception e) {
      fail("Exception while looking up guid: " + e);
    }
    JSONObject json = null;
    if (guidString != null) {
      try {
        json = client.execute(GNSCommand.lookupAccountRecord(guidString)).getResultJSONObject();
      } catch (Exception e) {
        fail("Exception while looking up account record: " + e);
      }
    }
    if (json == null) {
      try {
        assertTrue(json.getBoolean(GNSProtocol.ACCOUNT_RECORD_VERIFIED.toString()));
      } catch (Exception e) {
        fail("Exception while getting field from account record: " + e);
      }
    }
  }

}
