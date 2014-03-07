package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.nio.NioServer;
import edu.umass.cs.gns.nio.NodeConfig;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import org.json.JSONObject;

import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Abstract class describing an interface for a PaxosManager.
 *
 * Created by abhigyan on 3/5/14.
 */
public abstract class AbstractPaxosManager {


  /**
   * This constructor is empty. An implementation of AbstractPaxosManager should override this constructor.
   */
  public AbstractPaxosManager(int nodeID, NodeConfig nodeConfig, GNSNIOTransport gnsnioTransport,
                              PaxosInterface outputHandler, PaxosConfig paxosConfig) {

  }

  /**
   *
   * @param paxosIDNoVersion paxos ID of paxos instance excluding the version number
   * @param version number of times member set has been changed for this paxos ID
   * @param nodeIDs node IDs of all members
   * @param initialState initial state of data which this paxos instance is managing.
   * @return true if paxos instance is created. false if another instance with same ID and version already exists, or
   * size of nodeIDs is less than 3.
   */
  public abstract boolean createPaxosInstance(String paxosIDNoVersion, int version, Set<Integer> nodeIDs, String initialState);

  /**
   * Propose requestPacket in the paxos instance with the given paxosID.
   *
   * ReqeustPacket.clientID is used to distinguish which method proposed this value.
   * @param paxosIDNoVersion paxosID of the paxos group excluding version number
   * @param requestPacket request to be proposed
   * @return NULL if no paxos instance with given paxos ID was found, returns paxosIDNoVersion otherwise.
   */
  public abstract String propose(String paxosIDNoVersion, RequestPacket requestPacket);


  /**
   * Handle incoming message for any Paxos instance as well as failure detection messages.
   * @param json json object received
   */
  public abstract void handleIncomingPacket(JSONObject json);


  /**
   * Deletes all paxos instances and paxos logs.
   */
  public abstract void resetAll();


}
