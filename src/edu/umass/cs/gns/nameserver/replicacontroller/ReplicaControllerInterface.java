/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nameserver.replicacontroller;

import java.util.Set;

/**
 *
 * @author westy
 */
public interface ReplicaControllerInterface {

  public ReplicaControllerRecord getNameRecordPrimary(String name);

  public void addNameRecordPrimary(ReplicaControllerRecord recordEntry);

  public void updateNameRecordPrimary(ReplicaControllerRecord recordEntry);

  public void removeNameRecord(String name);

  public Set<ReplicaControllerRecord> getAllPrimaryNameRecords();

  public void reset();
}
