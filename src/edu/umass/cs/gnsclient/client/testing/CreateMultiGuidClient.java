package edu.umass.cs.gnsclient.client.testing;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.VerificationException;

/**
 * @author gaozy
 *
 */
public class CreateMultiGuidClient {

  private static String ACCOUNT_ALIAS = "@gigapaxos.net";
  private static GNSClientCommands client;
  private static int NUM_CLIENT = 100;
  private static int BENIGN_CLIENT = 100;

  private final static int numThread = 10;
  //private final static String filename = "./scripts/activeCode/noop.js"; //"/Users/gaozy/WebStorm/test.js"; //
  //private final static String mal_file = "./scripts/activeCode/mal.js"; // "/Users/gaozy/WebStorm/mal.js"; //

  private static int createdGuid = 0;

  static synchronized void incr() {
    createdGuid = createdGuid + 1;
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    int node = Integer.parseInt(args[0]);
    BENIGN_CLIENT = Integer.parseInt(args[1]);
    NUM_CLIENT = Integer.parseInt(args[2]);
    boolean flag = Boolean.parseBoolean(args[3]);

    //Read in the code and serialize
//		String code = new String(Files.readAllBytes(Paths.get(filename)));
//		String code64 = Base64.encodeToString(code.getBytes("utf-8"), true);
//		String mal_code = new String(Files.readAllBytes(Paths.get(mal_file)));		
//		String mal_code64 = Base64.encodeToString(mal_code.getBytes("utf-8"), true);
    client = new GNSClientCommands();
    ExecutorService executor = Executors.newFixedThreadPool(numThread);

    for (int i = 0; i < NUM_CLIENT; i++) {
      String name = "test" + (node * 1000 + i) + ACCOUNT_ALIAS;

      GuidEntry guidAccount = null;

      try {
        guidAccount = lookupOrCreateAccountGuid(client, name, "password");
      } catch (VerificationException e) {
        // ignore verification exceptions
      } catch (Exception e) {
        System.out.println("Exception during accountGuid creation: " + e);
        System.exit(1);
      }

      System.out.println(name + ":" + guidAccount.getGuid());

      if (flag) {
        if (i < BENIGN_CLIENT) {
          executor.execute(new createGuidThread(client, guidAccount.getGuid(), guidAccount));
        } else {
          executor.execute(new createGuidThread(client, guidAccount.getGuid(), guidAccount));
        }
      }
    }

    while (createdGuid < NUM_CLIENT) {
      System.out.println(createdGuid + "/" + NUM_CLIENT + " guids have been created ...");
      Thread.sleep(1000);
    }

    System.out.println("Created all " + NUM_CLIENT + " guids.");
    System.exit(0);
  }

  static class createGuidThread implements Runnable {

    GNSClientCommands client;
    //String code;
    String guid;
    GuidEntry guidAccount;

    createGuidThread(GNSClientCommands client, // String code, 
            String guid, GuidEntry guidAccount) {
      this.client = client;
      //this.code = code;
      this.guid = guid;
      this.guidAccount = guidAccount;
    }

    @Override
    public void run() {
      String guid = guidAccount.getGuid();

      JSONObject json;
      try {
        json = new JSONObject("{\"nextGuid\":\"hello\",\"cnt\":1}");
        client.update(guidAccount, json);
        //client.activeCodeClear(guid, "read", guidAccount);
        //client.activeCodeSet(guid, "read", code.getBytes(), guidAccount);
        //JSONObject result = client.read(guidAccount);
      } catch (JSONException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (ClientException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      //String field = client.fieldRead(guidAccount, "nextGuid");
      CreateMultiGuidClient.incr();
    }

  }

  /**
   * Creates and verifies an account GUID. Yes it cheats on verification
   * using a backdoor built into the GNS server.
   *
   * @param client
   * @param name
   * @return a guid entry
   * @throws Exception
   */
  private static GuidEntry lookupOrCreateAccountGuid(GNSClientCommands client,
          String name, String password) throws Exception {
    GuidEntry guidEntry = KeyPairUtils.getGuidEntry("server.gns.name", name);
    if (guidEntry == null || !guidExists(client, guidEntry)) { // also handle case where it has been deleted from database
      guidEntry = client.accountGuidCreate(name, password);
      //client.accountGuidVerify(guidEntry, createVerificationCode(name));
      return guidEntry;
    } else {
      return guidEntry;
    }
  }

  private static boolean guidExists(GNSClientCommands client, GuidEntry guid)
          throws IOException {
    try {
      client.lookupGuidRecord(guid.getGuid());
    } catch (Exception e) {
      return false;
    }
    return true;
  }

//  private static final int VERIFICATION_CODE_LENGTH = 3; // Six hex characters
//  // this is so we can mimic the verification code the server is generting
//  // AKA we're cheating... if the SECRET changes on the server side 
//  // you'll need to change it here as well
//  private static final String SECRET = Config.getGlobalString(GNSClientConfig.GNSCC.VERIFICATION_SECRET);
//
//  private static String createVerificationCode(String name) {
//    return ByteUtils.toHex(Arrays.copyOf(SHA1HashFunction.getInstance().hash(name + SECRET), VERIFICATION_CODE_LENGTH));
//  }

}
