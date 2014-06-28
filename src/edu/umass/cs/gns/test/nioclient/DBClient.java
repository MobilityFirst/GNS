package edu.umass.cs.gns.test.nioclient;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.JSONMessageExtractor;
import edu.umass.cs.gns.nio.NIOTransport;
import edu.umass.cs.gns.nio.InterfaceNodeConfig;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Set;

/**
 * DBClient is an alternate interface to use GNS, which we expect to be used only for testing purposes. This interface
 * provides a minimal set of commands, in fact exactly four commands of read, write, add, and remove for a given name.
 *
 * The interface provides an asynchronous API. There is a public method to send requests in a JSON format.
 * The callback is an object of type BasicPacketDemultiplexer, which is provided as a constructor parameter.
 *
 * An instance of the client communicates with only one local name server, which is specified at creation time.
 * The client listens on a given port to receive responses from the LNS. It informs the local name server know of its
 * listening port by putting the port number in the outgoing requests.
 *
 * Created by abhigyan on 6/19/14.
 */
class DBClient {

  public static final String DEFAULT_PORT_FIELD = "_port";

  private final InetAddress lnsAddress;
  private final int lnsPort;
  private final int myPort;
  private final NIOTransport nioTransport;

  /**
   * Constructor
   * @param lnsAddress address of LNS
   * @param lnsPort  port at which LNS is listening
   * @param myPort port on which this client should listen for responses from LNS
   * @param demux the callback object for responses from LNS.
   * @throws IOException
   */
  public DBClient(InetAddress lnsAddress, final int lnsPort, final int myPort, AbstractPacketDemultiplexer demux)
          throws IOException {
    this.lnsAddress = lnsAddress;
    this.lnsPort = lnsPort;
    this.myPort = myPort;
    this.nioTransport = new NIOTransport(0, new InterfaceNodeConfig() {
      @Override
      public boolean containsNodeInfo(int ID) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Set<Integer> getNodeIDs() {
        throw new UnsupportedOperationException();
      }

      @Override
      public InetAddress getNodeAddress(int ID) {
        try {
          return InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
          e.printStackTrace();
        }
        return null;
      }

      @Override
      public int getNodePort(int ID) {
        return myPort;
      }
    }, new JSONMessageExtractor(demux));
    new Thread(nioTransport).start();
  }

  /**
   * Send requests to LNS in a JSON format. How to construct these JSON objects can be understood from the
   * {@link edu.umass.cs.gns.test.nioclient.ClientSample}.
   */
  public void sendRequest(JSONObject jsonObject) throws IOException, JSONException {
    GNS.getLogger().fine("Client sent request ... " + jsonObject);
    jsonObject.put(DEFAULT_PORT_FIELD, myPort);
    String headeredMsg = JSONMessageExtractor.prependHeader(jsonObject.toString());
    this.nioTransport.send(new InetSocketAddress(lnsAddress, lnsPort), headeredMsg.getBytes());
  }


}
