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
import edu.umass.cs.gnscommon.utils.Base64;

import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;

import org.apache.commons.lang3.RandomUtils;

import org.junit.Assert;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Comprehensive functionality test for the GNS.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class WriteReadBytesTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands = null;
  private static GuidEntry masterGuid;

  /**
   *
   */
  public WriteReadBytesTest() {
    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
        clientCommands.setForceCoordinatedReads(true).setNumRetriesUponTimeout(1);
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception while trying to create the client: " + e);
      }

      try {
        masterGuid = GuidUtils.getGUIDKeys(globalAccountName);
      } catch (Exception e) {
        Utils.failWithStackTrace("Exception while trying to create account guid: " + e);
      }
    }
  }

  private static final String TEST_FIELD = "testBytes" + RandomString.randomString(12);
  private static byte[] testValue;

  /**
   *
   */
  @Test
  public void test_01_CreateBytesField() {
    try {
      testValue = RandomUtils.nextBytes(16000);
      String encodedValue = Base64.encodeToString(testValue, true);
      //System.out.println("Encoded string: " + encodedValue);
      clientCommands.fieldUpdate(masterGuid, TEST_FIELD, encodedValue);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception during create field: " + e);
    }
  }

  /**
   *
   */
  @Test
  public void test_02_ReadBytesField() {
    try {
      String string = clientCommands.fieldRead(masterGuid, TEST_FIELD);
      //System.out.println("Read string: " + string);
      Assert.assertArrayEquals(testValue, Base64.decode(string));
    } catch (ClientException | IOException e) {
      Utils.failWithStackTrace("Exception while reading field: " + e);
    }
  }
}
