function sendRequest() {
	// body...
	var url = "http://localhost:24703/GNS/lookupguid?name=usaxena";
	var representationOfDesiredState = "The cheese is old and moldy, where is the bathroom?";

	var client = new XMLHttpRequest();

	client.open("GET", url);

	// client.setRequestHeader("Content-Type", "text/plain");
	client.setRequestHeader("Access-Control-Allow-Credentials", '*');

	client.send();
	// client.onloadend = function() {
    		// if (client.readyState == 4) {
        			// doSomethingWith(http.responseText);
        			// alert(client.responseText);
    		// }
	// }
	alert(client.responseText);
	
	// if (client.status == 200)
 //    		alert("The request succeeded!\n\nThe response representation was:\n\n" + client.responseText)
	// else
 //    		alert("The request did not succeed!\n\nThe response status was: " + client.status + " " + client.statusText + ".");
}