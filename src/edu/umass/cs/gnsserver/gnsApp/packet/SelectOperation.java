/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.gnsApp.packet;

/**
 * The select operation.
 */
public enum SelectOperation {
  /**
   * Special case query for field with value.
   */
  EQUALS, /**
   * Special case query for location field near point.
   */
  NEAR, /**
   * Special case query for location field within bounding box.
   */
  WITHIN, /**
   * General purpose query.
   */
  QUERY

}
