Prerequisites: `JRE1.8+`, `bash`, `mongodb` (optional)

Start a single-node, local server as
```
bin/gpServer.sh restart all
```

Start the command-line interface (CLI) client as 
```
bin/cli.sh
```

The steps for other server configurations are similar and just need to specify the respective server and client properties files using the `-DgigapaxosConfig=` option.

Upon starting the CLI client, you should see output like below with a prompt
```
GNS Client Version: 1.17_2016-5-16_build2010
Connected to GNS.
GNS CLI - Connected to GNS>
```

Create an account as follows using your own email address:
```
GNS CLI - Connected to GNS>account_create support@gns.name some_password
Created an account with GUID 673B591F0558A4699493CA5BFF7FC8DFE4466347.
GNS CLI - support@gns.name>
```

The above will email a verification code to the specified email address if email verification is enabled. You can enable it by removing the `-disableEmailVerification` flag in the properties file `conf/gnsserver.1local.properties` being implicitly used here. If email verification is enabled, you would need to enter the verification code from the email as follows; if not, you can skip to the next step.
```
GNS CLI - support@gns.name>account_verify support@gns.name 596E11
Account verified.
```
 
Create a field in the created GUID as
```
GNS CLI - support@gns.name>field_update count 12345
Value '12345' written to field count for GUID 1FFCBCD4DC0E59840BA2E5313239C5240A296734
```

Now read it back as
```
GNS CLI - support@gns.name>read
{"count":"12345"}
```

Type `help` to see a list of all supported commands.