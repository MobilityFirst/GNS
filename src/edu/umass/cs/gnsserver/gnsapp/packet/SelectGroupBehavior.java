/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.gnsapp.packet;

/**
 * The group behavior for select operations.
 */
public enum SelectGroupBehavior {
  /**
   * Normal query, just returns results.
   */
  NONE, //
  /**
   * Set up a group guid that satisfies general purpose query.
   */
  GROUP_SETUP, //
  /**
   * Lookup value of group guid with an associated query.
   */
  GROUP_LOOKUP

}
