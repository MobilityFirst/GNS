package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.packet.paxospacket.PValuePacket;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by abhigyan on 1/9/14.
 */
public class PaxosCoordinator {
//  /**
//   * If activeCoordinator == true, this replica is coordinator.
//   * otherwise I will drop any proposals that I receive.
//   */
//   boolean activeCoordinator = false;

  /**
   * Coordinator has got {@code coordinatorBallot} accepted by majority of nodes.
   */
  Ballot coordinatorBallot;

//  /**
//   * Lock controlling access to {@code coordinatorBallotLock}
//   */
//   final ReentrantLock coordinatorBallotLock = new ReentrantLock();

  /**
   * {@code ballotScout} is the ballot currently proposed by coordinator
   * to get accepted by majority of replicas. When majority of replicas
   * accept {@code ballotScout}, {@code ballotScout} is copied to {@code coordinatorBallot} &
   * {@code ballotScout} is useless after that.
   */
  Ballot ballotScout;

//  /**
//   * object used to synchronize access to {@code ballotScout}, {@code waitForScout},
//   * and {@code pValuesScout}.
//   */
//   final ReentrantLock scoutLock = new ReentrantLock();

  /**
   * List of replicas who have accepted the ballot {@code ballotScout}
   */
  ArrayList<Integer> waitForScout;

  /**
   * Set of pValues received from replicas who have have accepted {@code ballotScout}.
   * Key = slot number, Value = PValuePacket, where PValuePacket contains the command that
   * was proposed by previous coordinator(s) for the slot number.
   *
   * A pValue is a tuple <slotNumber, ballot, request>. If multiple replicas respond
   * with a pValue for a given slot, the pValue with the highest ballot is accepted.
   * Once the ballot {@code ballotScout} is accepted by majority of replicas,
   * the coordinator proposes these commands again in the same slot numbers.
   */
  HashMap<Integer,PValuePacket> pValuesScout;

  /**
   * Slot number for the next proposal by this coordinator.
   */
  int nextProposalSlotNumber = 0;

//  /**
//   * Object synchronizing access to {@code nextProposalSlotNumber}
//   */
//   final ReentrantLock proposalNumberLock = new ReentrantLock();

  /**
   * Stores information about commands that are currently proposed by
   * the coordinator, but not yet accepted. Key = slot number, Value = information
   * about command proposed in slot number.
   */
  HashMap<Integer, ProposalStateAtCoordinator> pValuesCommander
          = new HashMap<Integer, ProposalStateAtCoordinator>(2,2);

//  /**
//   * Stores mapping from nodes and their current slot numbers. State before
//   * minimum slot numbers across nodes can be garbage collected.
//   */
//   int[] nodeAndSlotNumbers;

  // PAXOS-STOP
  /**
   * True if stop command is proposed by coordinator. Once stop command is decided in a slot,
   * the coordinator does not propose any commands in any higher numbered slots.
   */
  boolean stopCommandProposed = false;

  int[] nodeSlotNumbers;

//  int maxSlotNumberAtReplica = -1;

  /**
   * Timeout value after which accept messages for a slot are resent to nodes who have not yet responded
   */
  static long RESEND_TIMEOUT = 1000;


}
