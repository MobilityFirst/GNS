$(document).ready(function(){
$("#request_send_1").click(function() {
  var x = document.getElementById("input_box_1").value;
	$.ajax({
    
    url: "http://127.0.0.1:24703/GNS/lookupguid",
    data: {"name":x},
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
        // document.getElementById("box").innerHTML = JSON.stringify(response);
      }
});
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
        // alert(response);
        var data = parseData(response);
        document.getElementById("data").innerHTML = data;
        return response;        
      },
      error: function(response) {
        console.log(response);
      }
});
});

$("#request_send_12").click(function() {
var x = document.getElementById("input_box_12").value;
var jsonObject = {};
jsonObject["COMMANDINT"] = 160;
// jsonObject["field"] = "+ALL+";
// jsonObject["field"] = "occupation";
// jsonObject["field"] = String(x);
jsonObject["field"] = x;
// jsonObject["field"] = "name";

jsonObject["guid"] = guid; // target guid - user@gns.name 
jsonObject["reader"] = reader; // querier guid -- reader@gns.name
// jsonObject["seqnum"] = randomString(32); // insert random request nonce
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
        // alert("Access Denied");
        document.getElementById("result").innerHTML = "Error: " + "Access Denied";
      } else {
        // alert(x + " : " + response);
        document.getElementById("result").innerHTML = "Field Value: " + response;
      }
        return response;        
      },
      error: function(response) {
        // alert(JSON.stringify(response));
        // console.log(response);
      }
});
});

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
});