var HelpEsb = require('../help-esb');
var client = new HelpEsb.Client(process.env.ESB);
client.login('bar');
client.send('asdf', {name: 'cool guy'});

client.on('type.error', function(err) {
  console.warn('Oh noes!');
  console.warn(err);
});
