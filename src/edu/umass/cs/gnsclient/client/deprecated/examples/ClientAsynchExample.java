
package edu.umass.cs.gnsclient.client.deprecated.examples;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GUIDUtilsHTTPClient;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.packets.ResponsePacket;
import edu.umass.cs.gnscommon.utils.ThreadUtils;
import org.json.JSONObject;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static edu.umass.cs.gnsclient.client.CryptoUtils.createAndSignCommand;


public class ClientAsynchExample {

  private static final String ACCOUNT_ALIAS = "gnstest@cs.umass.edu"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS


  public static void main(String[] args) throws IOException,
          InvalidKeySpecException, NoSuchAlgorithmException, ClientException,
          InvalidKeyException, SignatureException, Exception {

    // Create the client
    GNSClientCommands client = new GNSClientCommands(null);
    GuidEntry accountGuidEntry = null;
    try {
      // Create a guid (which is also an account guid)
      accountGuidEntry = GUIDUtilsHTTPClient.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, "password", true);
    } catch (Exception e) {
      System.out.println("Exception during accountGuid creation: " + e);
      e.printStackTrace();
      System.exit(1);
    }
    System.out.println("Client connected to GNS");

    JSONObject command;
    if (args.length > 0 && args[0].equals("-write")) {
      JSONObject json = new JSONObject("{\"occupation\":\"busboy\","
              + "\"friends\":[\"Joe\",\"Sam\",\"Billy\"],"
              + "\"gibberish\":{\"meiny\":\"bloop\",\"einy\":\"floop\"},"
              + "\"location\":\"work\",\"name\":\"frank\"}");
      command = createAndSignCommand(CommandType.ReplaceUserJSON,
              accountGuidEntry.getPrivateKey(),
              accountGuidEntry.getPublicKey(),
              GNSProtocol.GUID.toString(), accountGuidEntry.getGuid(), GNSProtocol.USER_JSON.toString(), json.toString(),
              GNSProtocol.WRITER.toString(), accountGuidEntry.getGuid());
    } else {
      command = createAndSignCommand(CommandType.Read,
              accountGuidEntry.getPrivateKey(),
              accountGuidEntry.getPublicKey(),
              GNSProtocol.GUID.toString(), accountGuidEntry.getGuid(), GNSProtocol.FIELD.toString(), "occupation",
              GNSProtocol.READER.toString(), accountGuidEntry.getGuid());
    }
    // Create the command packet with a bogus id
    // arun: can not change request ID
    CommandPacket commandPacket = new CommandPacket((long) (Math.random() * Long.MAX_VALUE), command);
    // Keep track of what we've sent for the other thread to look at.
    Set<Long> pendingIds = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
    // Create and run another thread to pick up the responses
    Runnable companion = new Runnable() {
      @Override
      public void run() {
        lookForResponses(client, pendingIds);
      }
    };
    //Does this on Android as of 9/16:
    //ERROR: ClientAsynchExample.java:114: Lambda coming from jar file need their interfaces 
    //on the classpath to be compiled, unknown interfaces are java.lang.Runnable
//    Runnable companion = () -> {
//      lookForResponses(client, pendingIds);
//    };
    new Thread(companion).start();
    while (true) {
      //long id = client.generateNextRequestID();
      // Important to set the new request id each time
      //commandPacket.setClientRequestId(id);
      // Record what we're sending
      pendingIds.add(commandPacket.getRequestID());
      // arun: disabled
      if (true) {
        throw new RuntimeException("disabled");
      }
      // Actually send out the packet
      //client.sendCommandPacketAsynch(commandPacket);
      ThreadUtils.sleep(100); // if you generate them too fast you'll clog things up 
    }
  }

  // Not saying this is the best way to handle responses, but it works for this example.
  private static void lookForResponses(GNSClient client, Set<Long> pendingIds) {
    while (true) {
      ThreadUtils.sleep(10);
      // Loop through all the ones we've sent
      for (Long id : pendingIds) {
        if (true //client.isAsynchResponseReceived(id)
                ) {
          // arun: disabled
          if (true) {
            throw new RuntimeException("disabled");
          }
          pendingIds.remove(id);
          Request removed = null;//client.removeAsynchResponse(id);
          if (removed instanceof ResponsePacket) {
            ResponsePacket commandResult = ((ResponsePacket) removed);
            System.out.println("commandResult for  " + id + " is "
                    + (commandResult.getErrorCode().equals(ResponseCode.NO_ERROR)
                    ? commandResult.getReturnValue()
                    : commandResult.getErrorCode().toString())
            //                  + "\n"
            //                  + "Latency is " + commandResult.getClientLatency()
            );
          }
        }
      }
    }
  }

}
