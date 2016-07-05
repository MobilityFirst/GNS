package edu.umass.cs.gnsserver.interfaces;

import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author arun
 *
 */
public interface ActiveDBInterface {
  /**
   * @param querierGuid
   * @param queriedGuid
   * @param field
   *
   * @return ValuesMap representation of guid.field.
   */
  public ValuesMap read(String querierGuid, String queriedGuid, String field);

  /**
   * @param querierGuid
   * @param queriedGuid
   * @param field
   * @param valuesMap
   * @return True if write succeeded.
   */
  public boolean write(String querierGuid, String queriedGuid, String field, ValuesMap valuesMap);
}
