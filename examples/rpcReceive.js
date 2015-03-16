var HelpEsb = require('../help-esb');
var Promise = require('bluebird');
var client = new HelpEsb.Client(process.env.ESB, {debug: true});
client.login('rpcReceive');

client.rpcReceive('rpc-test', function(message) {
  return {greeting: 'Hello ' + message.get('name')};
});

client.on('type.error', function(err) {
  console.warn('Oh noes!');
  console.warn(err);
});
