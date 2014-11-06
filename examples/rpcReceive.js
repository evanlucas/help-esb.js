var HelpEsb = require('../help-esb');
var Promise = require('bluebird');
var client = new HelpEsb.Client(process.env.ESB);
client.login('rpcReceive');
client.subscribe('rpc-test');

client.rpcReceive('rpc-test', function(data) {
  return {greeting: 'Hello ' + data.name};
});

client.on('type.error', function(err) {
  console.warn('Oh noes!');
  console.warn(err);
});
