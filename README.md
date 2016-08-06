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

A simple client side example can be found in ActiveCodeHelloWorldExample.java. It creates an GNS account first, then use the account to create a field. After the field is created, it reads the field without active code. Then it deploys its own active code on single field read operation. In the end, the client reads the field again with the active code deployed.
To run the client with no-op active code, use the following command:
```bash
./scripts/client/runActiveLocalClient \
edu.umass.cs.gnsclient.client.testing.activecode.ActiveCodeHelloWorldExample
```
This no-op active code returns the original value of the field without any change, the output should look like:
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

## Performance Test
You could use another client to test ActiveGNS throughput as:
```bash
./scripts/client/runClientSingleNode \
-DtestingConfig=testing_conf/SomeReadsEncryptionFails.properties \
edu.umass.cs.gnsclient.client.testing.GNSClientCapacityTest \
NUM_CLIENTS=40 NUM_REQUESTS=400000
```

## More active code examples
The active code supported by ActiveGNS should be written in Javascript, and it must implement the following function:
```Javascript
function run(value, field, querier) {
	return value;
}
```
where _field_ is a string, _value_ is a [JSONObject](http://docs.oracle.com/javaee/7/api/javax/json/JsonObject.html) with the queried field and its corresponding value in it,  _querier_ is an object which allows user-defined code to query some other users field. 
_querier_ is a Java object with two public methods available for the user:
```Java
ValuesMap readGuid(String guid, String field) throws ActiveException;
void writeGuid(String guid, String field, ValuesMap value) throws ActiveException;
```
The type ValuesMap is an extension of JSONObject, see [ValuesMap](https://github.com/MobilityFirst/GNS/blob/master/src/edu/umass/cs/gnsserver/utils/ValuesMap.java) for the details. 
* _readGuid_ allows customer's Javascript code to read the value of _field_, from _guid_. The parameter _guid_ could be the same as customer's own GUID, and it does not require any ACL check to read the value of _field_. If _guid_ is different from the customer's own GUID, the customer must make sure he is permitted to read the _field_ . Otherwise, he will get an exception to indicate that the read operation is not allowed.
* The method _writeGuid_ allows customer's code to write _value_ into _field_ of _guid_. If the parameter _guid_ is the same as customer's own GUID, then there is no need for ACL check, and the _field_ will be updated. If _guid_ is different from the customer's own GUID, the customer must make sure he is permitted to write into the _field_ of _guid_. Otherwise, he will get an exception to indicate that the write operation is not allowed.

A use example of these two methods is like:
```Javascript
function run(value, field, querier) {
    var newValue = querier.readGuid("guid", "field");
    querier.writeGuid(null, "myField");
	return value;
}
```



## Prohibitions
Any method that may bring harm to Active GNS system are all forbidden. The misbehavior we could identified right now are listed below. Please do not make the following calls as it may lead to a punishment if your misbehavior is detected.
* Active GNS do not allow customer code call Java native class, even though the Javascript code running in Nashorn could call the method of Java native class through Java.type and Java.extend methods. But we disable Java hooks in global scope by using "--no-java" flag when Nashorn script engine is initialized. Therefore, if the customer code contains Java native class, the code execution will result in failure.
* Active GNS has no way to prevent customer code to allocating a huge amount of memory, and thus causing Active GNS worker JVM throws an OutOfMemoryError and terminates immediately. Please do not try to use your code to misbehave in this way. Such a misbehavior will lead to a severe result as customer guid will be blacklisted immediately as soon as it is detected.
