---
---

The Shibboleth SP Demo is currently being hosted at https://ec2-54-149-184-126.us-west-2.compute.amazonaws.com/secure/

When you visit that page or any other page in the /secure/ directory you will be asked to authenticate with the UMass Identity Provider, you can login with your university credentials.

To create a new GUID for testing purposes please visit the following url and replace ACCOUNT_NAME with the name you'd like to create. https://ec2-54-149-184-126.us-west-2.compute.amazonaws.com/secure/create.php?user=ACCOUNT_NAME

Then to update the location of that user visit the following url and replace ACCOUNT_NAME with the user you created earlier and LOCATION_NAME with the location you wish to set the user as having entered. https://ec2-54-149-184-126.us-west-2.compute.amazonaws.com/secure/update.php?user=ACCOUNT_NAME&location=LOCATION_NAME
This will currently only display some debug information about the account being found, but it will properly update your location.

To read all of the users at a given location visit the following url replacing LOCATION_NAME with the location you want to query over
https://ec2-54-149-184-126.us-west-2.compute.amazonaws.com/secure/read.php?location=LOCATION_NAME
This will return some debug information, but at the end it will display a list of all the users found at the location.  For example, it may say "Found the following people in lgrc: ["bteich"] ["bteich2"]" when visiting https://ec2-54-149-184-126.us-west-2.compute.amazonaws.com/secure/read.php?location=lgrc.  This signifies that it found two users at this location: bteich and bteich2.