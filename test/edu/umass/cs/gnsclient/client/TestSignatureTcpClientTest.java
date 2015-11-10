/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnsclient.client.UniversalTcpClientExtended;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.GnsProtocol;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;
import java.net.InetSocketAddress;
import java.security.PrivateKey;
import java.security.PublicKey;
import org.json.JSONObject;
import static org.junit.Assert.*;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Signature functionality test for the GNS Tcp client.
 * 
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestSignatureTcpClientTest {

  private static String ACCOUNT_ALIAS = "westy@cs.umass.edu"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String privateKeyFile = "/Users/westy/pkcs8_key";
  private static UniversalTcpClient client;
  private static GuidEntry guid;
  /**
   * The address of the GNS server we will contact
   */
  private static InetSocketAddress address = null;

  public TestSignatureTcpClientTest() {
    if (address == null) {
      address = ServerSelectDialog.selectServer();
      client = new UniversalTcpClientExtended(address.getHostName(), address.getPort(), true);
      // Retrive the GUID using the account id
      String guidString;
      try {
        guidString = client.lookupGuid(ACCOUNT_ALIAS);
      } catch (Exception e) {
        e.printStackTrace();
        return;
      }
      try {
        System.out.println("Retrieved GUID for " + ACCOUNT_ALIAS + ": " + guidString);
      } catch (Exception e) {
        e.printStackTrace();
        return;
      }
      PublicKey publicKey;
      try {
        // Get the public key from the GNS
        publicKey = client.publicKeyLookupFromGuid(guidString);
      } catch (Exception e) {
        e.printStackTrace();
        return;
      }
      PrivateKey privateKey;
      System.out.println("Retrieved public key: " + publicKey.toString());
      try {
        privateKey = KeyPairUtils.getPrivateKeyFromPKCS8File(privateKeyFile);
      } catch (Exception e) {
        e.printStackTrace();
        return;
      }
      System.out.println("Retrieved private key: " + privateKey.toString());
      guid = new GuidEntry(ACCOUNT_ALIAS, guidString, publicKey, privateKey);
      System.out.println("Created GUID entry: " + guid.toString());
    }
  }

  @Test
  @Order(1)
  public void test_01() {
    try {
      JSONObject command = client.createAndSignCommand(guid.getPrivateKey(), GnsProtocol.READ_ARRAY,
              GnsProtocol.GUID, guid.getGuid(), GnsProtocol.FIELD, "joe");
      System.out.println(command);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: " + e);
    }
  }
}
