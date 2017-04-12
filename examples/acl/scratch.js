

// // select query
// $("#request_send_4").click(function() {
//   $.ajax({
//     url: "http://localhost:24703/GNS/selectquery",
//     data: {"query":"\"~name\":\"Udit\""},
//     type: 'GET',
//     crossDomain: true,
//     dataType: 'jsonp',
//     success: function(response) {
//         alert(response);
//         return response;        
//       },
//       error: function(response) {
//         console.log(response);
//         // console.log();
//       }
// });
// });

// $("#request_send_5").click(function() {
//   private_key_1 = "-----BEGIN RSA PRIVATE KEY-----\
// MIICXAIBAAKBgQDCFp1nkBqf1CbAVFvDZRCm3JeBBmKNN0Vv1nBTkuEUHy5JfF6V\
// G25KbzZ1Z8TTTebZokEafommdAQAha1F5FhJZnqe7X07ZJnUDL231vsNMfXoW1Y2\
// vdrALsB1PGOrXL+FYgVmrwoR8SS2OYpwVCYfNmvtK713uWG+jW5tjqeSrQIDAQAB\
// AoGAaI5YQ1cdEKbwLUIEoRFL1CgXsdkntB1nWaUyo8MHX5igjdPi+/5n/s9EjiDV\
// pmNcDjfbTJOIQdRT55gbzQADEq8i0s4ClzE0WsAe6RRIx2MBzHuHuKkSZHwFREL4\
// 8IFGS58MLlNEh/uxeT/JpkNPG8Z98EsKZgEcAPre/FhZjEECQQDp2iQLWZINLpkl\
// 1r2d5chqb8gPSp5OwNDCt5dTwbCDo5u240EncApFqvqtIdJjJn/Mt9BoZ1+zG4Fu\
// uVA4XntxAkEA1Hhhwm9EaF4zP6TGAaRoYGecKG0qMnIL6nO7QQWr+BZQ0Bf6VOcu\
// CKQ9LKidYiRXdpGvijbigfpgit9SWO7U/QJAVJBYpm4OfYvTP7amvPmB/tNLQhRW\
// qN3a3/7pzxTkksXQIlONlQhT/pt0qLTpUodygYIa8BYpqigRJwwGMUwhUQJBAI9u\
// nQA3QQT74rjqMUlWhaffCgo4d64KU4T4j8a7X3ZhCjkQlsvOLMNkrG+DfIuOYQUI\
// IGq71nlMXvQhAmRLBrkCQCQDlQeYbkDy6BnMajK7BIJQTuTOgHo33pEbJ/pqSTm2\
// 8/g4F/owY5OgtH+KWZCg6foW0ChlpbQniBQFq5J0Kjs=\
// -----END RSA PRIVATE KEY-----";

// var jsonObject = {};
// jsonObject["COMMANDINT"] = 160;
// // jsonObject["field"] = "+ALL+";
// // jsonObject["field"] = "occupation";
// jsonObject["field"] = "location";
// // jsonObject["field"] = "name";
// // jsonObject["guid"] = "911CB5C917973913A5B7D9766B4CC9C37C9A5A03"; // target guid - barry@gns 
// // jsonObject["reader"] = "7E79B4008C9CBCD4305B3CC0FCCF90D8BD509B80"; // querier guid -- udit@gns

// jsonObject["guid"] = "642BFEC43CA0926A40FF18675C4A24BF2FDA47BA"; // target guid - user@gns.name 
// // jsonObject["reader"] = "642BFEC43CA0926A40FF18675C4A24BF2FDA47BA"; // querier guid -- reader@gns.name
// jsonObject["reader"] = "7CE03A1C4F14254C44F859829AAC126DEAAA1981"; // querier guid -- reader@gns.name
// jsonObject["seqnum"] = "1234567890";
// // jsonObject["seqnum"] = randomString(32); // insert random request nonce
// // now = moment().utc().format("YYYY-MM-DDTHH:mm:ss") + "X"
// // now = moment().utc().format("YYY-MM-DDTHH:mm:ss")
// jsonObject["timestamp"] = "2017-03-24T08:15:24-04";//moment().format("yyyy,MM,dd, T, HH, mm, ss,");//moment("2010-01-01T05:06:07", moment.ISO_8601);//moment().format();//new Date().toISOString(); // insert iso8601 utc
// var message = JSON.stringify(jsonObject);

// // alert(message);
// console.log(message);
// // console.log(escape(encodeURI(message)));
// // alert(private_key);
// var sig = new KJUR.crypto.Signature({"alg": "SHA1withRSA"});
// // initialize for signature generation
// sig.init(private_key_1);   // rsaPrivateKey of RSAKey object
// // sig.init(private_key_2);   // rsaPrivateKey of RSAKey object
// // update data
// sig.updateString(message);
// // calculate signature
// var sigValueHex = sig.sign();
// // alert(sigValueHex);
// console.log("\n"+sigValueHex.toUpperCase() + "\n\n");

// sigValueHex = sigValueHex.toUpperCase();

// console.log("\n"+btoa(sigValueHex) + "\n\n");

// jsonObject["signature"]=btoa(sigValueHex);
// console.log(jsonObject);
//   $.ajax({
//     url: "http://localhost:24703/GNS/read",
//     cache:true,
//     jsonp: false,
//     data: jsonObject,
//     type: 'GET',
//     crossDomain: true,
//     dataType: 'jsonp',

//     success: function(response) {
//         alert(response);
//         return response;        
//       },
//       error: function(response) {
//         console.log(response);
//       }
// });
// });

// var randomString = function(length) {
//     var text = "";
//     var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
//     for(var i = 0; i < length; i++) {
//         text += possible.charAt(Math.floor(Math.random() * possible.length));
//     }
//     return text;
// };
// $("#request_send_6").click(function() {
// now = moment().utc().format("YYYY-MM-DDTHH:mm:ss") + "Z";
// // alert(now);
// });

// $("#request_send_7").click(function() {

// var jsonObject = {};
// jsonObject["COMMANDINT"] = 160;
// // jsonObject["field"] = "+ALL+";
// // jsonObject["field"] = "occupation";
// // jsonObject["field"] = "location";
// jsonObject["field"] = "name";

// jsonObject["guid"] = guid; // target guid - user@gns.name 
// jsonObject["reader"] = reader; // querier guid -- reader@gns.name
// // jsonObject["seqnum"] = randomString(32); // insert random request nonce
// now = moment().utc().format("YYYY-MM-DDTHH:mm:ss") + "Z"; // require the format: "2017-03-28T14:35:24Z";
// jsonObject["timestamp"] = now;
// var message = JSON.stringify(jsonObject);
// var sig = new KJUR.crypto.Signature({"alg": "SHA1withRSA"});
// // initialize for signature generation
// sig.init(reader_key);   // rsaPrivateKey of RSAKey object
// // update data
// sig.updateString(message);
// // calculate signature
// var sigValueHex = sig.sign();
// sigValueHex = sigValueHex.toUpperCase();
// jsonObject["signature"]=btoa(sigValueHex);
// console.log(jsonObject);
//   $.ajax({
//     url: "http://localhost:24703/GNS/read",
//     cache:true,
//     jsonp: false,
//     data: jsonObject,
//     type: 'GET',
//     crossDomain: true,
//     dataType: 'text',
//     // jsonpCallback:"logResults",
//     success: function(response) {
//         alert(response);
//         return response;        
//       },
//       error: function(response) {
//         // alert(JSON.stringify(response));
//         // console.log(response);
//       }
// });
// });

// $("#request_send_9").click(function() {

// var jsonObject = {};
// jsonObject["COMMANDINT"] = 515;
// // jsonObject["field"] = "+ALL+";
// // jsonObject["field"] = "occupation";
// jsonObject["field"] = "location";
// // jsonObject["field"] = "name";

// jsonObject["guid"] = guid; // target guid - user@gns.name 
// jsonObject["aclType"] = "READ_WHITELIST"; // querier guid -- reader@gns.name
// // jsonObject["reader"] = reader; // querier guid -- reader@gns.name
// // jsonObject["seqnum"] = randomString(32); // insert random request nonce
// now = moment().utc().format("YYYY-MM-DDTHH:mm:ss") + "Z"; // require the format: "2017-03-28T14:35:24Z";
// jsonObject["timestamp"] = now;
// var message = JSON.stringify(jsonObject);
// var sig = new KJUR.crypto.Signature({"alg": "SHA1withRSA"});
// // initialize for signature generation
// sig.init(account_key);   // rsaPrivateKey of RSAKey object
// // update data
// sig.updateString(message);
// // calculate signature
// var sigValueHex = sig.sign();

// sigValueHex = sigValueHex.toUpperCase();
// jsonObject["signature"]=btoa(sigValueHex);
// console.log(jsonObject);
//   $.ajax({
//     url: "http://localhost:24703/GNS/aclretrieveself",
//     cache:true,
//     jsonp: false,
//     data: jsonObject,
//     type: 'GET',
//     crossDomain: true,
//     dataType: 'jsonp',
//     // jsonpCallback:"logResults",
//     success: function(response) {
//         alert(response);
//         return response;        
//       },
//       error: function(response) {
//         // alert(JSON.stringify(response));
//         // console.log(response);
//       }
// });
// });

// $("#request_send_10").click(function() {

// var jsonObject = {};
// jsonObject["COMMANDINT"] = 517;
// // jsonObject["field"] = "+ALL+";
// // jsonObject["field"] = "occupation";
// // jsonObject["field"] = "location";
// jsonObject["field"] = "name";

// jsonObject["guid"] = guid; // target guid - user@gns.name 
// jsonObject["aclType"] = "READ_WHITELIST"; 
// // jsonObject["accessor"] = reader; // querier guid -- reader@gns.name
// // jsonObject["seqnum"] = randomString(32); // insert random request nonce
// now = moment().utc().format("YYYY-MM-DDTHH:mm:ss") + "Z"; // require the format: "2017-03-28T14:35:24Z";
// jsonObject["timestamp"] = now;
// var message = JSON.stringify(jsonObject);
// var sig = new KJUR.crypto.Signature({"alg": "SHA1withRSA"});
// // initialize for signature generation
// sig.init(account_key);   // rsaPrivateKey of RSAKey object
// // update data
// sig.updateString(message);
// // calculate signature
// var sigValueHex = sig.sign();
// sigValueHex = sigValueHex.toUpperCase();
// jsonObject["signature"]=btoa(sigValueHex);
// console.log(jsonObject);
//   $.ajax({
//     url: "http://localhost:24703/GNS/fielddeleteacl",
//     cache:true,
//     jsonp: false,
//     data: jsonObject,
//     type: 'GET',
//     crossDomain: true,
//     dataType: 'text',
//     // jsonpCallback:"logResults",
//     success: function(response) {
//         alert(response);
//         return response;        
//       },
//       error: function(response) {
//         // alert(JSON.stringify(response));
//         // console.log(response);
//       }
// });
// });


// $("#request_send_11").click(function() {

// var jsonObject = {};
// jsonObject["COMMANDINT"] = 510;
// // jsonObject["field"] = "+ALL+";
// // jsonObject["field"] = "occupation";
// jsonObject["field"] = "location";
// // jsonObject["field"] = "name";

// jsonObject["guid"] = guid; // target guid - user@gns.name 
// jsonObject["reader"] = reader; // querier guid -- reader@gns.name
// // jsonObject["seqnum"] = randomString(32); // insert random request nonce
// now = moment().utc().format("YYYY-MM-DDTHH:mm:ss") + "Z"; // require the format: "2017-03-28T14:35:24Z";
// jsonObject["timestamp"] = now;
// var message = JSON.stringify(jsonObject);
// var sig = new KJUR.crypto.Signature({"alg": "SHA1withRSA"});
// // initialize for signature generation
// sig.init(account_key);   // rsaPrivateKey of RSAKey object
// // update data
// sig.updateString(message);
// // calculate signature
// var sigValueHex = sig.sign();
// sigValueHex = sigValueHex.toUpperCase();
// jsonObject["signature"]=btoa(sigValueHex);

// console.log(jsonObject);
//   $.ajax({
//     url: "http://localhost:24703/GNS/acladd",
//     cache:true,
//     jsonp: false,
//     data: jsonObject,
//     type: 'GET',
//     crossDomain: true,
//     dataType: 'jsonp',
//     success: function(response) {
//         alert(response);
//         return response;        
//       },
//       error: function(response) {
//         // alert(JSON.stringify(response));
//         // console.log(response);
//       }
// });
// });
