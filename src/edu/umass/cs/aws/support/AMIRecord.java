package edu.umass.cs.aws.support;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author westy
 */
public class AMIRecord {
  private String name;
  private String description;
  private String instanceType;

  private AMIRecord(String name, String description, String instanceType) {
    this.name = name;
    this.description = description;
    this.instanceType = instanceType;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public String getInstanceType() {
    return instanceType;
  }
  public static Map<AMIRecordType, Map<RegionRecord, AMIRecord>> records = null;

  public static AMIRecord getAMI(AMIRecordType type, RegionRecord region) {
    if (records == null) {
      init();
    }
    //System.out.print("RECORDS: " + records.toString());
    System.out.print("AMI for " + type.toString() + " and Region " + region.toString());
    AMIRecord result = records.get(type).get(region);
    System.out.println(" is " + result != null ? result.getName() : "NULL!");
    return result;
  }

  public static void init() {
    records = new EnumMap<AMIRecordType, Map<RegionRecord, AMIRecord>>(AMIRecordType.class);
    // our new mongo AMI
    HashMap<RegionRecord, AMIRecord> amiMap_Mongo_2014_5_6_micro = new HashMap<RegionRecord, AMIRecord>();
    amiMap_Mongo_2014_5_6_micro.put(RegionRecord.US_EAST_1, new AMIRecord("ami-0479996c", "Mongo_2014_5_6", "t1.micro"));
    amiMap_Mongo_2014_5_6_micro.put(RegionRecord.US_WEST_2, new AMIRecord("ami-b7fd8a87", "Mongo_2014_5_6", "t1.micro"));
    amiMap_Mongo_2014_5_6_micro.put(RegionRecord.US_WEST_1, new AMIRecord("ami-fad4efbf", "Mongo_2014_5_6", "t1.micro"));
    amiMap_Mongo_2014_5_6_micro.put(RegionRecord.EU_WEST_1, new AMIRecord("ami-dfef29a8", "Mongo_2014_5_6", "t1.micro"));
    amiMap_Mongo_2014_5_6_micro.put(RegionRecord.AP_SOUTHEAST_1, new AMIRecord("ami-f26331a0", "Mongo_2014_5_6", "t1.micro"));
    amiMap_Mongo_2014_5_6_micro.put(RegionRecord.AP_NORTHEAST_1, new AMIRecord("ami-d18dcad0", "Mongo_2014_5_6", "t1.micro"));
    amiMap_Mongo_2014_5_6_micro.put(RegionRecord.AP_SOUTHEAST_2, new AMIRecord("ami-e5a03bdf", "Mongo_2014_5_6", "t1.micro"));
    amiMap_Mongo_2014_5_6_micro.put(RegionRecord.SA_EAST_1, new AMIRecord("ami-ab54f9b6", "Mongo_2014_5_6", "t1.micro"));
    records.put(AMIRecordType.Mongo_2014_5_6_micro, amiMap_Mongo_2014_5_6_micro);
    
    HashMap<RegionRecord, AMIRecord> amiMap_Mongo_2014_5_6_small = new HashMap<RegionRecord, AMIRecord>();
    amiMap_Mongo_2014_5_6_small.put(RegionRecord.US_EAST_1, new AMIRecord("ami-0479996c", "Mongo_2014_5_6", "t2.small"));
    amiMap_Mongo_2014_5_6_small.put(RegionRecord.US_WEST_2, new AMIRecord("ami-b7fd8a87", "Mongo_2014_5_6", "t2.small"));
    amiMap_Mongo_2014_5_6_small.put(RegionRecord.US_WEST_1, new AMIRecord("ami-fad4efbf", "Mongo_2014_5_6", "t2.small"));
    amiMap_Mongo_2014_5_6_small.put(RegionRecord.EU_WEST_1, new AMIRecord("ami-dfef29a8", "Mongo_2014_5_6", "t2.small"));
    amiMap_Mongo_2014_5_6_small.put(RegionRecord.AP_SOUTHEAST_1, new AMIRecord("ami-f26331a0", "Mongo_2014_5_6", "t2.small"));
    amiMap_Mongo_2014_5_6_small.put(RegionRecord.AP_NORTHEAST_1, new AMIRecord("ami-d18dcad0", "Mongo_2014_5_6", "t2.small"));
    amiMap_Mongo_2014_5_6_small.put(RegionRecord.AP_SOUTHEAST_2, new AMIRecord("ami-e5a03bdf", "Mongo_2014_5_6", "t2.small"));
    amiMap_Mongo_2014_5_6_small.put(RegionRecord.SA_EAST_1, new AMIRecord("ami-ab54f9b6", "Mongo_2014_5_6", "t2.small"));
    records.put(AMIRecordType.Mongo_2014_5_6_small, amiMap_Mongo_2014_5_6_small);
    // our new mongo AMI
    HashMap<RegionRecord, AMIRecord> amiMap_Mongo_2014_5_6 = new HashMap<RegionRecord, AMIRecord>();
    amiMap_Mongo_2014_5_6.put(RegionRecord.US_EAST_1, new AMIRecord("ami-0479996c", "Mongo_2014_5_6", "m1.small"));
    amiMap_Mongo_2014_5_6.put(RegionRecord.US_WEST_2, new AMIRecord("ami-b7fd8a87", "Mongo_2014_5_6", "m1.small"));
    amiMap_Mongo_2014_5_6.put(RegionRecord.US_WEST_1, new AMIRecord("ami-fad4efbf", "Mongo_2014_5_6", "m1.small"));
    amiMap_Mongo_2014_5_6.put(RegionRecord.EU_WEST_1, new AMIRecord("ami-dfef29a8", "Mongo_2014_5_6", "m1.small"));
    amiMap_Mongo_2014_5_6.put(RegionRecord.AP_SOUTHEAST_1, new AMIRecord("ami-f26331a0", "Mongo_2014_5_6", "m1.small"));
    amiMap_Mongo_2014_5_6.put(RegionRecord.AP_NORTHEAST_1, new AMIRecord("ami-d18dcad0", "Mongo_2014_5_6", "m1.small"));
    amiMap_Mongo_2014_5_6.put(RegionRecord.AP_SOUTHEAST_2, new AMIRecord("ami-e5a03bdf", "Mongo_2014_5_6", "m1.small"));
    amiMap_Mongo_2014_5_6.put(RegionRecord.SA_EAST_1, new AMIRecord("ami-ab54f9b6", "Mongo_2014_5_6", "m1.small"));
    records.put(AMIRecordType.Mongo_2014_5_6, amiMap_Mongo_2014_5_6);
    // AMAZON LINUX
    HashMap<RegionRecord, AMIRecord> amiMap_2014_03_1 = new HashMap<RegionRecord, AMIRecord>();
    amiMap_2014_03_1.put(RegionRecord.US_EAST_1, new AMIRecord("ami-fb8e9292", "Amazon Linux AMI 2014.03.1", "t1.micro"));
    amiMap_2014_03_1.put(RegionRecord.US_WEST_1, new AMIRecord("ami-7aba833f", "Amazon Linux AMI 2014.03.1", "t1.micro"));
    amiMap_2014_03_1.put(RegionRecord.US_WEST_2, new AMIRecord("ami-043a5034", "Amazon Linux AMI 2014.03.1", "t1.micro"));
    amiMap_2014_03_1.put(RegionRecord.EU_WEST_1, new AMIRecord("ami-2918e35e", "Amazon Linux AMI 2014.03.1", "t1.micro"));
    amiMap_2014_03_1.put(RegionRecord.AP_SOUTHEAST_1, new AMIRecord("ami-b40d5ee6", "Amazon Linux AMI 2014.03.1", "t1.micro"));
    amiMap_2014_03_1.put(RegionRecord.AP_NORTHEAST_1, new AMIRecord("ami-c9562fc8", "Amazon Linux AMI 2014.03.1", "t1.micro"));
    amiMap_2014_03_1.put(RegionRecord.AP_SOUTHEAST_2, new AMIRecord("ami-3b4bd301", "Amazon Linux AMI 2014.03.1", "t1.micro"));
    amiMap_2014_03_1.put(RegionRecord.SA_EAST_1, new AMIRecord("ami-215dff3c", "Amazon Linux AMI 2014.03.1", "t1.micro"));
    records.put(AMIRecordType.Amazon_Linux_AMI_2014_03_1, amiMap_2014_03_1);
    // AMAZON LINUX
    HashMap<RegionRecord, AMIRecord> amiMap_2013_09_2 = new HashMap<RegionRecord, AMIRecord>();
    amiMap_2013_09_2.put(RegionRecord.US_EAST_1, new AMIRecord("ami-bba18dd2", "Amazon Linux AMI 2013.09.2", "t1.micro"));
    amiMap_2013_09_2.put(RegionRecord.US_WEST_1, new AMIRecord("ami-a43909e1", "Amazon Linux AMI 2013.09.2", "t1.micro"));
    amiMap_2013_09_2.put(RegionRecord.US_WEST_2, new AMIRecord("ami-ccf297fc", "Amazon Linux AMI 2013.09.2", "t1.micro"));
    amiMap_2013_09_2.put(RegionRecord.EU_WEST_1, new AMIRecord("ami-5256b825", "Amazon Linux AMI 2013.09.2", "t1.micro"));
    amiMap_2013_09_2.put(RegionRecord.AP_SOUTHEAST_1, new AMIRecord("ami-b4baeee6", "Amazon Linux AMI 2013.09.2", "t1.micro"));
    amiMap_2013_09_2.put(RegionRecord.AP_NORTHEAST_1, new AMIRecord("ami-0d13700c", "Amazon Linux AMI 2013.09.2", "t1.micro"));
    amiMap_2013_09_2.put(RegionRecord.AP_SOUTHEAST_2, new AMIRecord("ami-5ba83761", "Amazon Linux AMI 2013.09.2", "t1.micro"));
    amiMap_2013_09_2.put(RegionRecord.SA_EAST_1, new AMIRecord("ami-c99130d4", "Amazon Linux AMI 2013.09.2", "t1.micro"));
    records.put(AMIRecordType.Amazon_Linux_AMI_2013_09_2, amiMap_2013_09_2);
    
    HashMap<RegionRecord, AMIRecord> amiMap_2013_03_1 = new HashMap<RegionRecord, AMIRecord>();
    amiMap_2013_03_1.put(RegionRecord.US_EAST_1, new AMIRecord("ami-05355a6c", "Amazon Linux AMI 2013.03.1", "t1.micro"));
    amiMap_2013_03_1.put(RegionRecord.US_WEST_1, new AMIRecord("ami-3ffed17a", "Amazon Linux AMI 2013.03.1", "t1.micro"));
    amiMap_2013_03_1.put(RegionRecord.US_WEST_2, new AMIRecord("ami-0358ce33", "Amazon Linux AMI 2013.03.1", "t1.micro"));
    amiMap_2013_03_1.put(RegionRecord.EU_WEST_1, new AMIRecord("ami-c7c0d6b3", "Amazon Linux AMI 2013.03.1", "t1.micro"));
    amiMap_2013_03_1.put(RegionRecord.AP_SOUTHEAST_1, new AMIRecord("ami-fade91a8", "Amazon Linux AMI 2013.03.1", "t1.micro"));
    amiMap_2013_03_1.put(RegionRecord.AP_NORTHEAST_1, new AMIRecord("ami-39b23d38", "Amazon Linux AMI 2013.03.1", "t1.micro"));
    amiMap_2013_03_1.put(RegionRecord.AP_SOUTHEAST_2, new AMIRecord("ami-d16bfbeb", "Amazon Linux AMI 2013.03.1", "t1.micro"));
    amiMap_2013_03_1.put(RegionRecord.SA_EAST_1, new AMIRecord("ami-5253894f", "Amazon Linux AMI 2013.03.1", "t1.micro"));
    records.put(AMIRecordType.Amazon_Linux_AMI_2013_03_1, amiMap_2013_03_1);
    // MongoDB 2.4.8 with 1000 IOPS
    HashMap<RegionRecord, AMIRecord> amiMap2 = new HashMap<RegionRecord, AMIRecord>();
    amiMap2.put(RegionRecord.US_EAST_1, new AMIRecord("ami-f1416498", "MongoDB 2.4.8 with 1000 IOPS", "m1.large"));
    records.put(AMIRecordType.MongoDB_2_4_8_with_1000_IOPS, amiMap2);
  }
}
