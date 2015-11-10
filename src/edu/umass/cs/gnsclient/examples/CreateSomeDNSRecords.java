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
package edu.umass.cs.gnsclient.examples;

import edu.umass.cs.gnsclient.client.BasicUniversalTcpClient;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;
import edu.umass.cs.gnsclient.exceptions.GnsException;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.net.InetSocketAddress;

/**
 * Creates some DNS records for testing the GNS DNS service.
 *
 * @author westy
 */
public class CreateSomeDNSRecords {
  
  

  private static final String ACCOUNT_ALIAS = "westy@cs.umass.edu"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static BasicUniversalTcpClient client;
  private static GuidEntry accountGuid;
  
  private static boolean disableSSL = true;

  public static void main(String[] args) throws IOException,
          InvalidKeySpecException, NoSuchAlgorithmException, GnsException,
          InvalidKeyException, SignatureException, Exception {

    InetSocketAddress address;
    if (args.length == 0 || (address = ServerSelectDialog.parseHostPortTuple(args[0])) == null) {
      address = ServerSelectDialog.selectServer();
    }
    client = new BasicUniversalTcpClient(address.getHostName(), address.getPort(), disableSSL);
    try {
      accountGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, "password", true);
    } catch (Exception e) {
      System.out.println("Exception during accountGuid creation: " + e);
      System.exit(1);
    }
    System.out.println("Client connected to GNS at " + address.getHostName() + ":" + address.getPort());

    createAnARecord("westy.gns.", "173.236.153.191");
    
    createAnARecord("mobilityfirst.gns.", "128.119.240.104");
    
    client.stop();
    
    System.exit(0);
  }
  
  // Creates an A record in the GNS
  private static void createAnARecord(String domain, String address) throws Exception {
    // domains need to end with a period
    if (!domain.endsWith(".")) {
      domain = domain + ".";
      
    }
    GuidEntry guid = GuidUtils.lookupOrCreateGuid(client, accountGuid, domain);
    client.fieldUpdate(guid, "A", address);
    System.out.println("Value of A in " + guid.getEntityName() + " is " + client.fieldRead(guid, "A"));
    
  }

}
