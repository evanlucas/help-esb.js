var HelpEsb = require('../help-esb');
var client = new HelpEsb.Client(process.env.ESB, {debug: true});
client.login('bar');
client.send('asdf', {name: 'cool guy'}).finally(client.close.bind(client));

client.on('type.error', function(err) {
  console.warn('Oh noes!');
  console.warn(err);
});
