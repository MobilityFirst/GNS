/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.gnsApp.packet;

/**
 * The group behavior.
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
