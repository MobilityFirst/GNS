
package edu.umass.cs.aws.support;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.route53.AmazonRoute53;
import com.amazonaws.services.route53.AmazonRoute53Client;
import com.amazonaws.services.route53.model.Change;
import com.amazonaws.services.route53.model.ChangeAction;
import com.amazonaws.services.route53.model.ChangeBatch;
import com.amazonaws.services.route53.model.ChangeResourceRecordSetsRequest;
import com.amazonaws.services.route53.model.ChangeResourceRecordSetsResult;
import com.amazonaws.services.route53.model.ListResourceRecordSetsRequest;
import com.amazonaws.services.route53.model.ListResourceRecordSetsResult;
import com.amazonaws.services.route53.model.RRType;
import com.amazonaws.services.route53.model.ResourceRecord;
import com.amazonaws.services.route53.model.ResourceRecordSet;

import java.util.ArrayList;
import java.util.List;


public class Route53 {

  private static AmazonRoute53 route53;
  private static final String CALLER_REFERENCE = "GNS";
  private static final String HOSTED_ZONE_ID = "Z10LUHH9502F33";


  public static void main(String[] args) throws Exception {
    AWSCredentials credentials = new PropertiesCredentials(
            AWSEC2.class.getResourceAsStream("resources/AwsCredentials.properties"));
    //Create Amazon Client object
    route53 = new AmazonRoute53Client(credentials);
    listRecordSetsForHostedZone();
  }

//  private void test() {
//    route53.createHostedZone(new CreateHostedZoneRequest()
//            .withName("myDomainName.com")
//            .withCallerReference(CALLER_REFERENCE)
//            .withHostedZoneConfig(new HostedZoneConfig()
//            .withComment("my first Route 53 hosted zone!")));
//  }

  private static void listRecordSetsForHostedZone() {

    ListResourceRecordSetsRequest request = new ListResourceRecordSetsRequest();
    request.setHostedZoneId(HOSTED_ZONE_ID);

    ListResourceRecordSetsResult result = route53.listResourceRecordSets(request);
    List<ResourceRecordSet> recordSets = result.getResourceRecordSets();

    for (ResourceRecordSet recordSet : recordSets) {

      System.out.println(recordSet.toString());
    }
  }

  private static void createRecordSetFromHostedZone() {

    List<ResourceRecord> records = new ArrayList<>();
    ResourceRecord record = new ResourceRecord();
    record.setValue("http://www.marksdevserver.com");
    records.add(record);

    ResourceRecordSet recordSet = new ResourceRecordSet();
    recordSet.setName("markstest.domain.com.");
    recordSet.setType(RRType.CNAME);
    recordSet.setTTL(new Long(60));
    recordSet.setResourceRecords(records);

    // Create the Change
    List<Change> changes = new ArrayList<>();
    Change change = new Change();
    change.setAction(ChangeAction.CREATE);
    change.setResourceRecordSet(recordSet);
    changes.add(change);

    // Create a batch and add the change to it
    ChangeBatch batch = new ChangeBatch();
    batch.setChanges(changes);

    // Create a Request and add the batch to it.
    ChangeResourceRecordSetsRequest request = new ChangeResourceRecordSetsRequest();
    request.setHostedZoneId(HOSTED_ZONE_ID);
    request.setChangeBatch(batch);

    // send the request
    ChangeResourceRecordSetsResult result = route53.changeResourceRecordSets(request);
    System.out.println(result.toString());

  }
}
