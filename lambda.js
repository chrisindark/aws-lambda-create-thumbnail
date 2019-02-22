var lambda = require('./create-thumbnail.js');
var context = require('./context.js');
var event = require('./event.js');


var thisEvent = new event();
var thisContext = new context();

lambda.handler(thisEvent, thisContext);
