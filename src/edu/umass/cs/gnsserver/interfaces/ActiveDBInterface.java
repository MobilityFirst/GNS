package edu.umass.cs.gnsserver.interfaces;

import edu.umass.cs.gnsserver.gnsapp.recordmap.BasicRecordMap;
import edu.umass.cs.gnsserver.gnsapp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * @author arun
 *
 */
public interface ActiveDBInterface {
	
	/**
	 * FIXME: This is a temporary hack that will go away.
	 * @return DB
	 */
	public BasicRecordMap getDB();
	
	/**
	 * @param querierGuid 
	 * @param queriedGuid 
	 * @param field
	 * 
	 * FIXME: Explain why a NameRecord as opposed to ValuesMap
	 * is needed here.
	 *  
	 * @return ValuesMap representation of guid.field.
	 */
	public NameRecord read(String querierGuid, String queriedGuid, String field); 
	/**
	 * @param querierGuid 
	 * @param queriedGuid 
	 * @param field
	 * @param valuesMap
	 * @return True if write succeeded.
	 */
	public boolean write(String querierGuid, String queriedGuid, String field, ValuesMap valuesMap);
}
