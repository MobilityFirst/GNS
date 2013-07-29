/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.nameserver.recordmap;

import edu.umass.cs.gns.nameserver.NameRecord;
import edu.umass.cs.gns.nameserver.NameRecord;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Set;

/**
 *
 * @author westy
 */
public interface RecordMapInterface {

    public void addNameRecord(JSONObject json);

    public void removeNameRecord(String name);

    public boolean containsName(String name);

    public Set<String> getAllColumnKeys(String key);

    public Set<String> getAllRowKeys();

    public void updateNameRecordField(String name, String key, ArrayList<String> value);

    public String getNameRecordField(String name, String key);

    public String tableToString();

    public void reset();

    //
    // OLD Style
    //
    public void addNameRecord(NameRecord recordEntry);

    public NameRecord getNameRecord(String name);

    public Set<NameRecord> getAllNameRecords();

    public void updateNameRecord(NameRecord recordEntry);
}
