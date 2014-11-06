var HelpEsb = require('../help-esb');
var client = new HelpEsb.Client(process.env.ESB);
client.login('foo');
client.subscribe('asdf');

client.on('group.asdf', function(data) {
  console.log('Received data:');
  console.log(data);
});

client.on('type.error', function(err) {
  console.warn('Oh noes!');
  console.warn(err);
});
