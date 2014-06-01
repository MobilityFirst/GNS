package edu.umass.cs.gns.localnameserver;

/**
 * Codes for different events that may happens when processing a request at a local name server.
 * A request would go through a sequence of such events after being received at a local name server
 * until its processing is complete, i.e., either request is declared success or failed.
 *
 * The events are defined only for debugging purposes as they help us know how LNS handled a given request.
 * These events should not be used for implementing any functionality in the system.
 *
 * Created by abhigyan on 5/29/14.
 */
public enum LNSEventCode {
  CONTACT_RC, // no valid actives in cache; contact replica controllers (RC)
  CONTACT_ACTIVE ,  // send request to active
  MAX_WAIT_ERROR ,  // maximum wait time exceeded
  INVALID_ACTIVE_ERROR ,  // invalid active set received
  OTHER_ERROR,  // active returned other error
  RC_NO_RECORD_ERROR ,  // record does not exist at replica controllers
  RC_NO_RESPONSE_ERROR ,  // replica controllers did not respond on repeated attempts for active replicas of a name
  CACHE_HIT,  // cached response returned by LNS on a read
  SUCCESS  // success message recvd from name server
}
