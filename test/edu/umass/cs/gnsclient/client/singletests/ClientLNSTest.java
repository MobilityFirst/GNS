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

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.utils.Utils;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Tests the LNS using the GNS Proxy feature.
 *
 */
// This requires that the LOCAL_NAME_SERVER_NODES config option be set.
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ClientLNSTest extends DefaultGNSTest {

  private static GNSClientCommands clientCommands;

  /**
   *
   */
  public ClientLNSTest() {
    if (clientCommands == null) {
      try {
        clientCommands = new GNSClientCommands();
        //PaxosConfig.getActives() works here because the server and client use the same properties file.
        InetAddress lnsAddress = PaxosConfig.getActives().values().iterator().next().getAddress();
        clientCommands.setGNSProxy(new InetSocketAddress(lnsAddress, 24598));
        clientCommands.setForceCoordinatedReads(true);
      } catch (IOException e) {
        Utils.failWithStackTrace("Exception creating client: " + e);
      }
    }
  }

  /**
   *
   */
  @Test
  public void test_02_CheckAccount() {
    String guidString = null;
    try {
      guidString = clientCommands.lookupGuid(globalAccountName);
    } catch (IOException | ClientException e) {
      Utils.failWithStackTrace("Exception while looking up guid: " + e);
    }
    if (guidString != null) {
      try {
        clientCommands.lookupAccountRecord(guidString);
      } catch (IOException | ClientException e) {
        Utils.failWithStackTrace("Exception while looking up account record: " + e);
      }
    }
  }

}
