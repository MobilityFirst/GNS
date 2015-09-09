/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.gnsApp;

import edu.umass.cs.gigapaxos.InterfaceReplicable;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.Stringifiable;
import edu.umass.cs.reconfiguration.PaxosReplicaCoordinator;

/**
 * @author Westy
 * @param <String>
 */
public class GnsAppCoordinator<String> extends PaxosReplicaCoordinator<String> {

  GnsAppCoordinator(InterfaceReplicable app, Stringifiable<String> unstringer, 
          JSONMessenger<String> messenger) {
    super(app, messenger.getMyID(), unstringer, messenger);
  }
}
