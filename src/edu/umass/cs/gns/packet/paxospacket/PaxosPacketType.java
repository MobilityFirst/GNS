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
  public static final int REQUEST = 10;
  /**
   * Response from Paxos replica to client.
   */
  public static final int RESPONSE = 0;
  /**
   * 
   */
  public static final int PREPARE = 20;
  public static final int ACCEPT = 30;
  public static final int RESEND_ACCEPT = 40;
  public static final int PROPOSAL = 50;
  public static final int DECISION = 60;
  public static final int PREPARE_REPLY = 70;
  public static final int ACCEPT_REPLY = 80;
  public static final int FAILURE_DETECT = 90;
  public static final int FAILURE_RESPONSE = 100;
  public static final int NODE_STATUS = 110;
  public static final int SEND_STATE = 210;
  public static final int SEND_STATE_NO_RESPONSE = 220;
  public static final int REQUEST_STATE = 230;
  public static final int SYNC_REQUEST = 310;
  public static final int SYNC_REPLY = 320;
  public static final int START = 410;
  public static final int STOP = 420;
}
