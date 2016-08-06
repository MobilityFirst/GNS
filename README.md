# ActiveGNS
ActiveGNS is built upon Auspice GNS (Global Name Service), see [GNS document](https://github.com/MobilityFirst/GNS) for details about Auspice.

## Prerequisites
To fetch, compile, and run the code in this project, you need to make sure the following have been installed:
* [ant](http://ant.apache.org/)
* [Java 8](https://docs.oracle.com/javase/8/docs/technotes/guides/install/install_overview.html)

## Getting started with ActiveGNS
To fetch ActiveGNS from git:
```bash
git clone https://github.com/ZhaoyuUmass/ActiveGNS
```

After the repository is downloaded, cd into ActiveGNS folder and run ant to compile code:
```bash
ant
```

After the code is compile, you can start an single ActiveGNS server by using the script:
```bash
./scripts/activeLocal/reset_and_restart.sh
```

An "hello world" client side example can be found in src/edu/umass/cs/gnsclient/client/testing/activecode/ActiveCodeHelloWorldExample.java. It creates an GNS account first, then use the account to create a field. After the field is created, it reads the field without active code. Then it deploys its own active code on single field read operation. In the end, the client reads the field again with the active code deployed.
To run the client with no-op active code, use the following command:
```bash
./scripts/client/runActiveLocalClient \
edu.umass.cs.gnsclient.client.testing.activecode.ActiveCodeHelloWorldExample
```
This noop active code returns the original value of the field without any change, the output should look like:
>Before the code is deployed, the value of field(someField) is someValue
>After the code is deployed, the value of field(someField) is someValue

To run another simple hello world active code, you could specify the path to the code and run as:
```bash
./scripts/client/runActiveLocalClient \
edu.umass.cs.gnsclient.client.testing.activecode.ActiveCodeHelloWorldExample \
scripts/activeCode/HelloWorld.js
```

This code changes the original value of the field, and returns a new value as a String "hello world!". Its output should look like:
>Before the code is deployed, the value of field(someField) is someValue
>After the code is deployed, the value of field(someField) is hello world!


## More active code examples
TODO

## Prohibitions
TODO
