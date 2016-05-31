/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.gnamed;

/**
 * Which type of worker are we. Used by GnsDnsLookupTask.
 */
 enum WorkerClass {
  DNS, GNS, GNSLOCAL
  
}
