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
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.console;

import org.junit.Test;

/**
 * This class defines a MSocketTest
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class MSocketTest extends ConsoleBasedTest
{

  /**
   * Reproduces the steps to initialize an mSocket Proxy
   */
  @Test
  public void testmSocketProxyInitialization()
  {
    String inCommands;
    String expectedOutput;

    // Commands from ProxyGroupCreate
    inCommands = "guid_create proxy_group\n";
    inCommands += "field_create +" + GnsConstants.ACTIVE_PROXY_FIELD + "\n";
    inCommands += "field_create +" + GnsConstants.SUSPICIOUS_PROXY_FIELD + "\n";
    inCommands += "field_create +" + GnsConstants.INACTIVE_PROXY_FIELD + "\n";
    inCommands += "field_create +" + GnsConstants.ACTIVE_WATCHDOG_FIELD + "\n";
    inCommands += "field_create +" + GnsConstants.SUSPICIOUS_WATCHDOG_FIELD + "\n";
    inCommands += "field_create +" + GnsConstants.INACTIVE_WATCHDOG_FIELD + "\n";
    inCommands += "field_create +" + GnsConstants.ACTIVE_LOCATION_FIELD + "\n";
    inCommands += "field_create +" + GnsConstants.SUSPICIOUS_LOCATION_FIELD + "\n";
    inCommands += "field_create +" + GnsConstants.INACTIVE_PROXY_FIELD + "\n";

    expectedOutput = "Looking for alias proxy_group GUID and certificates...\n";
    expectedOutput += "Generating new GUID and keys for account .*\n";
    expectedOutput += "Created GUID .*\n";
    expectedOutput += "New field " + GnsConstants.ACTIVE_PROXY_FIELD + " created with value ''\n";
    expectedOutput += "New field " + GnsConstants.SUSPICIOUS_PROXY_FIELD + " created with value ''\n";
    expectedOutput += "New field " + GnsConstants.INACTIVE_PROXY_FIELD + " created with value ''\n";
    expectedOutput += "New field " + GnsConstants.ACTIVE_WATCHDOG_FIELD + " created with value ''\n";
    expectedOutput += "New field " + GnsConstants.SUSPICIOUS_WATCHDOG_FIELD + " created with value ''\n";
    expectedOutput += "New field " + GnsConstants.INACTIVE_WATCHDOG_FIELD + " created with value ''\n";
    expectedOutput += "New field " + GnsConstants.ACTIVE_LOCATION_FIELD + " created with value ''\n";
    expectedOutput += "New field " + GnsConstants.SUSPICIOUS_LOCATION_FIELD + " created with value ''\n";
    expectedOutput += "New field " + GnsConstants.INACTIVE_PROXY_FIELD + " created with value ''\n";

    runCommandsInConsole(inCommands, expectedOutput, true, true);
  }

}
