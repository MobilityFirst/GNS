package edu.umass.cs.gns.packet.paxospacket;

public class PaxosPacketType {
  
 /**
  * SEEMS LIKE THESE GET INLINED IN CLASSES.
  * IF YOU CHANGE THESE NUMBERS YOU NEED TO RECOMPILE EVERYTHING!!
  * MIGHT BE BETTER TO USE A ENUM FOR THIS TYPE OF STUFF ANYWAY.
  */

  public static final String ptype = "PT";
  /**
   * Request sent from client to Paxos Replica.
   */
  public static final int REQUEST = 1;
  /**
   * Response from Paxos replica to client.
   */
  public static final int RESPONSE = 0;
  /**
   * 
   */
  public static final int PREPARE = 2;
  public static final int ACCEPT = 3;
  public static final int RESEND_ACCEPT = 4;
  public static final int PROPOSAL = 5;
  public static final int DECISION = 6;
  public static final int PREPARE_REPLY = 7;
  public static final int ACCEPT_REPLY = 8;
  public static final int FAILURE_DETECT = 9;
  public static final int FAILURE_RESPONSE = 10;
  public static final int NODE_STATUS = 11;
  public static final int SEND_STATE = 21;
  public static final int SEND_STATE_NO_RESPONSE = 22;
  public static final int REQUEST_STATE = 23;
  public static final int SYNC_REQUEST = 31;
  public static final int SYNC_REPLY = 32;
  public static final int START = 41;
  public static final int STOP = 42;
}
