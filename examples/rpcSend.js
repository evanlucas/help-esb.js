var HelpEsb = require('../help-esb');
var client = new HelpEsb.Client(process.env.ESB, {debug: true});
client.login('rpcSend');

client.rpcSend('rpc-test', {name: 'nubs'})
  .timeout(5000)
  .then(function(response) {
    console.log('Received response:');
    console.log(response);
  }).catch(function(error) {
    console.warn('Received error:');
    console.warn(error);
  }).finally(client.close.bind(client));

client.on('type.error', function(err) {
  console.warn('Oh noes!');
  console.warn(err);
});
