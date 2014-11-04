var HelpEsb = require('../help-esb');
var client = new HelpEsb.Client(process.env.ESB_HOST, process.env.ESB_PORT);
client.login('bar');

setTimeout(function() {
  client.send('asdf', {name: 'cool guy'});
}, 2000);

client.on('error', function(err) {
  console.warn('Oh noes!');
  console.warn(err);
});
