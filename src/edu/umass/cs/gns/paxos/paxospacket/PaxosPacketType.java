package edu.umass.cs.gns.paxos.paxospacket;

import edu.umass.cs.gns.main.GNS;
import java.util.HashMap;
import java.util.Map;

public enum PaxosPacketType {
  
  REQUEST(1),
  //  
  PREPARE(2),
  ACCEPT(3),
  RESEND_ACCEPT(4),
  PROPOSAL(5),
  DECISION(6),
  PREPARE_REPLY(7), 
  ACCEPT_REPLY(8),
  FAILURE_DETECT(9),
  FAILURE_RESPONSE(10),
  NODE_STATUS(11),
  //
  SEND_STATE(21),
  SEND_STATE_NO_RESPONS(22),
  REQUEST_STATE(23),
  //
  SYNC_REQUEST(31),
  SYNC_REPLY(32),
  //
  START(41),
  STOP(42),
  // 
  NULL(99);
  
  private int number;
  
  private static final Map<Integer, PaxosPacketType> map = new HashMap<Integer, PaxosPacketType>();
    static {
      for (PaxosPacketType type : PaxosPacketType.values()) {
        if (map.containsKey(type.getInt())) {
          GNS.getLogger().warning("**** Duplicate ID number for packet type " + type + ": " + type.getInt());
        }
        map.put(type.getInt(), type);
      }
    }

    private PaxosPacketType(int number) {
      this.number = number;
    }

    public int getInt() {
      return number;
    }

    public static PaxosPacketType getPacketType(int number) {
      return map.get(number);
    }
   
  
  /**
   * Request sent from client to Paxos Replica.
   */
    
//  public static final int REQUEST = 1;
//  /**
//   * 
//   */
//  public static final int PREPARE = 2;
//  public static final int ACCEPT = 3;
//  public static final int RESEND_ACCEPT = 4;
//  public static final int PROPOSAL = 5;
//  public static final int DECISION = 6;
//  public static final int PREPARE_REPLY = 7;
//  public static final int ACCEPT_REPLY = 8;
//  public static final int FAILURE_DETECT = 9;
//  public static final int FAILURE_RESPONSE = 10;
//  public static final int NODE_STATUS = 11;
////  public static final int SEND_STATE = 21;
////  public static final int SEND_STATE_NO_RESPONSE = 22;
////  public static final int REQUEST_STATE = 23;
//  public static final int SYNC_REQUEST = 31;
//  public static final int SYNC_REPLY = 32;
//  public static final int START = 41;
//  public static final int STOP = 42;
}
