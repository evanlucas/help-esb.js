var HelpEsb = require('../help-esb');
var client = new HelpEsb.Client('54.165.246.18', 22);
client.subscribe('foo', ['asdf']);

client.on('payload', function(data) {
  console.log("Received data:");
  console.log(data);
});

client.on('error', function(err) {
  console.warn("Oh noes!");
  console.warn(err);
});
