$(document).ready(function(){
$("#request_send_1").click(function() {
	$.ajax({
    
    url: "http://127.0.0.1:24703/GNS/lookupguid",
    data: {"name":"user@gns.name"},
    type: 'GET',
    crossDomain: true,
    dataType: "text",
    success: function(response) {
      if (response.includes("+NO+")) {
        
        document.getElementById("result").innerHTML = "Error !";
      } else {
        
        document.getElementById("result").innerHTML = "GUID : " + response;
      }
        return response;        
      },
      error: function(response) {
        console.log(response);
      }
});

  clearText();
});

// look up guid record
$("#request_send_2").click(function() {
  var x = document.getElementById("input_box_2").value;
  $.ajax({
    url: "http://localhost:24703/GNS/lookupguidrecord",
    data: {"guid":x},
    type: 'GET',
    crossDomain: true,
    dataType: 'text',

    success: function(response) {
      if (response.includes("+NO+")) {
        
        document.getElementById("result").innerHTML = "Error !";
      } else {
        responseJSON = JSON.parse(response);
        document.getElementById("result").innerHTML = "alias : " + responseJSON.name;
      }
        return response;        
      },
      error: function(response) {
        console.log(response);
      }
});
  clearText();
});

//read Unsigned
$("#request_send_3").click(function() {
  $.ajax({
    url: "http://localhost:24703/GNS/readunsigned",
    data: {"guid":guid,"field":"+ALL+"},
    cache:true,
    jsonp: false,
    type: 'GET',
    crossDomain: true,
    dataType: 'text',

    success: function(response) {
        var data = parseData(response);
        document.getElementById("data").innerHTML = data;
        document.getElementById('result').innerHTML = "";
        return response;        
      },
      error: function(response) {
        console.log(response);
      }
});
  clearText();
});

$("#request_send_12").click(function() {
var x = document.getElementById("input_box_12").value;
if (!("name".includes(x) || "id".includes(x) || "location".includes(x) || "contact".includes(x) || "type".includes(x) || "status".includes(x))) {
    document.getElementById("result").innerHTML = "Error !";
  } else {
var jsonObject = {};
jsonObject["COMMANDINT"] = 160;
jsonObject["field"] = x;
jsonObject["guid"] = guid; // target guid - user@gns.name 
jsonObject["reader"] = reader; // querier guid -- reader@gns.name
now = moment().utc().format("YYYY-MM-DDTHH:mm:ss") + "Z"; // require the format: "2017-03-28T14:35:24Z";
jsonObject["timestamp"] = now;
var message = JSON.stringify(jsonObject);
var sig = new KJUR.crypto.Signature({"alg": "SHA1withRSA"});
// initialize for signature generation
sig.init(reader_key);   // rsaPrivateKey of RSAKey object
// update data
sig.updateString(message);
// calculate signature
var sigValueHex = sig.sign();
sigValueHex = sigValueHex.toUpperCase();
jsonObject["signature"]=btoa(sigValueHex);

console.log(jsonObject);
  $.ajax({
    url: "http://localhost:24703/GNS/read",
    cache:true,
    jsonp: false,
    data: jsonObject,
    type: 'GET',
    crossDomain: true,
    dataType: 'text',
    success: function(response) {
      if (response.includes("+ACCESS_DENIED+")) {
        document.getElementById("result").innerHTML = "Access Denied";
      } else if (response.includes("+GENERICERROR+")) {
        document.getElementById("result").innerHTML = "Error ! ";
      } else {
        document.getElementById("result").innerHTML = "Field Value: " + response;
      } 
        return response;        
      },
      error: function(response) {
         console.log(response);
      }
    
});
}
  clearText();
});


// Testing functions for acl add/remove
$("#request_send_16").click(function() {
  var x = document.getElementById("input_box_16").value;
  if (!("name".includes(x) || "id".includes(x) || "location".includes(x) || "contact".includes(x) || "type".includes(x) || "status".includes(x))) {
    document.getElementById("result").innerHTML = "Error !";
  } else {
  var jsonObject = {};
jsonObject["COMMANDINT"] = 511;
jsonObject["accesser"] = reader;
jsonObject["aclType"] = "READ_WHITELIST";
jsonObject["field"] = x;
jsonObject["guid"] = guid; // target guid - user@gns.name 

 // querier guid -- reader@gns.name
now = moment().utc().format("YYYY-MM-DDTHH:mm:ss") + "Z"; // require the format: "2017-03-28T14:35:24Z";
jsonObject["timestamp"] = now;
var message = JSON.stringify(jsonObject);
var sig = new KJUR.crypto.Signature({"alg": "SHA1withRSA"});
// initialize for signature generation
sig.init(account_key);   // rsaPrivateKey of RSAKey object
// update data
sig.updateString(message);
// calculate signature
var sigValueHex = sig.sign();
sigValueHex = sigValueHex.toUpperCase();
jsonObject["signature"]=btoa(sigValueHex);

console.log(jsonObject);
  $.ajax({
    url: "http://localhost:24703/GNS/acladdself",
    cache:true,
    jsonp: false,
    data: jsonObject,
    type: 'GET',
    crossDomain: true,
    dataType: 'text',
    success: function(response) {
      if (response.includes("+ACCESS_DENIED+")) {
        document.getElementById("result").innerHTML = "Error: " + "Access Denied";
      } else {
        if (response.includes("+OK+")) {
        document.getElementById("result").innerHTML = "Alias reader@gns.name added to ACL of : " + x+". Now reader can read the value of the field : " + x;;
      }
      }
        return response;        
      },
      error: function(response) {
         console.log(response);
      }
});
}
clearText();
});

$("#request_send_17").click(function() {
  allowed_list = ["name","id","location", "contact", "type", "status"]
  var x = document.getElementById("input_box_17").value;
  if (!("name".includes(x) || "id".includes(x) || "location".includes(x) || "contact".includes(x) || "type".includes(x) || "status".includes(x))) {
    document.getElementById("result").innerHTML = "Error !";
  } else {
  var jsonObject = {};
jsonObject["COMMANDINT"] = 513;
jsonObject["accesser"] = reader;
jsonObject["aclType"] = "READ_WHITELIST";
jsonObject["field"] = x;
jsonObject["guid"] = guid; // target guid - user@gns.name

 // querier guid -- reader@gns.name
now = moment().utc().format("YYYY-MM-DDTHH:mm:ss") + "Z"; // require the format: "2017-03-28T14:35:24Z";
jsonObject["timestamp"] = now;
var message = JSON.stringify(jsonObject);
var sig = new KJUR.crypto.Signature({"alg": "SHA1withRSA"});
// initialize for signature generation
sig.init(account_key);   // rsaPrivateKey of RSAKey object
// update data
sig.updateString(message);
// calculate signature
var sigValueHex = sig.sign();
sigValueHex = sigValueHex.toUpperCase();
jsonObject["signature"]=btoa(sigValueHex);
console.log(jsonObject);
  $.ajax({
    url: "http://localhost:24703/GNS/aclremoveself",
    cache:true,
    jsonp: false,
    data: jsonObject,
    type: 'GET',
    crossDomain: true,
    dataType: 'text',
    success: function(response) {
      if (response.includes("+ACCESS_DENIED+")) {
        // alert("Access Denied");
        document.getElementById("result").innerHTML = "Error: " + "Access Denied";
      } else {
        // alert(x + " : " + response);
        if (response.includes("+OK+")) {
        document.getElementById("result").innerHTML = "Alias reader@gns.name removed from ACL of : " + x +". Now reader can not read the value of the field : " + x;
      }
      }
        return response;        
      },
      error: function(response) {
        // alert(JSON.stringify(response));
        // console.log(response);
      }
});
}
clearText();
});

var doesAclExist = function(x, guid, reader) {

var jsonObject = {};
jsonObject["COMMANDINT"] = 518;
jsonObject["aclType"] = "READ_WHITELIST"; // querier guid -- reader@gns.name
jsonObject["field"] = "+ALL+";
jsonObject["guid"] = guid; // target guid - user@gns.name

jsonObject["reader"] = reader; // querier guid -- reader@gns.name
now = moment().utc().format("YYYY-MM-DDTHH:mm:ss") + "Z"; // require the format: "2017-03-28T14:35:24Z";
jsonObject["timestamp"] = now;
var message = JSON.stringify(jsonObject);
var sig = new KJUR.crypto.Signature({"alg": "SHA1withRSA"});
// initialize for signature generation
sig.init(account_key);   // rsaPrivateKey of RSAKey object
// update data
sig.updateString(message);
// calculate signature
var sigValueHex = sig.sign();

sigValueHex = sigValueHex.toUpperCase();
jsonObject["signature"]=btoa(sigValueHex);
console.log(jsonObject);
  $.ajax({
    url: "http://localhost:24703/GNS/fieldaclexists",
    cache:true,
    jsonp: false,
    data: jsonObject,
    type: 'GET',
    crossDomain: true,
    dataType: 'text',
    success: function(response) {
        alert(response);
      return response;          
      },
      error: function(response) {
         console.log(response);
        return response; 
      }

});
};

// end of testing functions
var parseData = function(jsonText) {
  parsedTest = JSON.parse(jsonText);
  var data = "";
  data += "name : " + parsedTest.name + "<br>"; 
  data += "id : " + parsedTest.id + "<br>"; 
  data +="location : " + parsedTest.location + "<br>"; 
  data +="contact : " + parsedTest.contact + "<br>"; 
  data +="type : " + parsedTest.type + "<br>"; 
  data +="status : " + parsedTest.status + "<br>"; 
  return data;
};

function clearText()  {
    document.getElementById('input_box_12').value = "";
    document.getElementById('input_box_16').value = "";
    document.getElementById('input_box_17').value = "";
};
});