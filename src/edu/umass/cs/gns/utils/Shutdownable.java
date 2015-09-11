/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.utils;

/**
 * Provides a means for a graceful shutdown.
 */
public interface Shutdownable {

  /**
   * Handle shutdown.
   */
  public void shutdown();
}
