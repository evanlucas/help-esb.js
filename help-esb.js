(function(root, factory) {
  'use strict';

  // Setup HelpEsb appropriately for the environment.  Dependency on net likely
  // means this only works on Node.js, but meh.
  factory(
    exports,
    require('net'),
    require('events').EventEmitter,
    require('util'),
    require('url'),
    require('bluebird'),
    require('uuid'),
    require('lodash')
  );
}(this, function(HelpEsb, net, EventEmitter, util, url, Promise, uuid, _) {
  'use strict';

  // ## HelpEsb.Client

  // ### HelpEsb.Client *constructor*
  // The client connects to the ESB running on the given host/port.  You will
  // need to [login](#helpesb-client-login) before doing anything over the
  // connection.
  //
  //     var client = Esb.Client('tcp://example.com:1234');
  //     client.login('clientName');
  //     client.subscribe('subscriptionChannel1');
  //     client.on('type.error', console.error);
  //     client.on('group.subscriptionChannel1', function(data) {
  //       // Process data
  //     });
  //
  // Or using the RPC conventions:
  //
  //     var client = Esb.Client('tcp://example.com:1234');
  //     client.login('clientName');
  //     client.rpcReceive('subscriptionChannel1', function(data) {
  //       // Process data
  //       return result;
  //     });
  HelpEsb.Client = function(uri) {
    // Extend EventEmitter to handle events.
    EventEmitter.call(this);

    this._connect(uri);

    // Start with no authentication and no subscriptions.
    this._authentication = null;
    this._subscriptions = {};
    this._login = null;
  };

  util.inherits(HelpEsb.Client, EventEmitter);

  // ### HelpEsb.Client.login
  // Login to the ESB using the given credentials (name only right now).
  // Returns a promise that gets resolved when successfully logged in.  This
  // same promise is kept internally as well for controlling when further
  // requests can be sent.
  //
  //     client.login('clientName');
  HelpEsb.Client.prototype.login = function(name) {
    this._login = name;

    return this._authentication = this._rpcSend({
      meta: {type: 'login'},
      data: {name: name, subscriptions: []}
    }).timeout(10000);
  };

  // ### HelpEsb.Client.subscribe
  // Subscribe to an ESB group.  This returns a
  // [promise](https://github.com/petkaantonov/bluebird) of the send event so
  // you can do additional tasks after the subscription has been sent.
  //
  //     client.subscribe('a').then(function() {
  //       console.log('Subscribed!');
  //     });
  HelpEsb.Client.prototype.subscribe = function(group) {
    if (typeof this._subscriptions[group] === 'undefined') {
      this._subscriptions[group] = this._authentication.then(function() {
        return this._rpcSend({
          meta: {type: 'subscribe'},
          data: {channel: group}
        }).timeout(10000);
      }.bind(this));
    }

    return this._subscriptions[group];
  };

  // ### HelpEsb.Client.send
  // Sends a payload message to the ESB with the given data.  Returns a promise
  // that, like the [subscribe](#helpesb-client-subscribe) call, is fulfilled
  // when the message is sent, but does not indicate whether the message was
  // received by the ESB or by any subscribers.  For RPC-esque behavior, use
  // [rpcSend](#helpesb-client-rpcsend).
  //
  //     client.send('target', {id: 1234, message: 'Hello!'});
  HelpEsb.Client.prototype.send = function(group, data, replyCallback) {
    return this._authentication.then(function() {
      return this._send(
        {meta: {type: 'sendMessage', group: group}, data: data},
        replyCallback
      );
    }.bind(this));
  };

  // ### HelpEsb.Client.rpcSend
  // Sends the packet like [send](#helpesb-client-send), but returns a promise
  // for a response from some other service.  This uses the autogen message id
  // and relies on the other service properly publishing a message with a
  // proper replyTo.
  //
  // Automatically subscribes to the result group for you if not already
  // subscribed.
  //
  //     client.rpcSend('foo', {name: 'John'}).then(function(response) {
  //       console.log(response);
  //     }).catch(function(error) {
  //       console.error(error);
  //     });
  HelpEsb.Client.prototype.rpcSend = function(group, data) {
    var send = Promise.promisify(HelpEsb.Client.prototype.send).bind(this);
    return this.subscribe(group + '-result').then(function() {
      return send(group, data).spread(this._checkRpcResult);
    }.bind(this));
  };

  // ### HelpEsb.Client.rpcReceive
  // Listen on the given group like [on](#helpesb-client-on), and call the
  // given callback with any messages.  The value returned by the callback is
  // sent to the GROUPNAME-result group in reply to the incoming message.
  //
  // Automatically subscribes to the group for you if not already subscribed.
  //
  //     client.rpcReceive('foo', function(data) {
  //       return {greeting: 'Hello ' + (data.name || 'Stranger')};
  //     });
  //
  // If the callback returns a promise, the result of the promise is sent.
  //
  //     client.rpcReceive('foo', function(data) {
  //       return request.getAsync('http://www.google.com');
  //     });
  //
  // Errors are also handled and errors are sent as the "reason" through the
  // ESB.
  //
  //     client.rpcReceive('foo', function(data) {
  //       throw new Error('Not implemented!');
  //     });
  HelpEsb.Client.prototype.rpcReceive = function(group, cb) {
    this.subscribe(group);
    this.on('group.' + group, function(data, incomingMeta) {
      // Link up our reply to the incoming request but on the "result" group.
      var meta = {
        type: 'sendMessage',
        group: group + '-result',
        replyTo: incomingMeta.id
      };

      // Catch thrown errors so that we can send the result through the ESB.
      var result = null;
      try {
        result = Promise.resolve(cb(data));
      } catch(e) {
        result = Promise.reject(e.toString());
      }

      result.then(function(data) {
        return this._send(
          {meta: _.extend({result: 'SUCCESS'}, meta), data: data}
        );
      }.bind(this)).catch(function(error) {
        return this._send(
          {
            meta: _.extend({result: 'FAILURE', reason: error}, meta),
            data: data
          }
        );
      }.bind(this));
    }.bind(this));
  };

  // ### HelpEsb.Client.close
  // Closes the connection, ending communication.
  HelpEsb.Client.prototype.close = function() {
    this.emit('socket.close');
    this._socket.removeAllListeners('close');
    this._socket.end();
  }

  // ---
  // ### Private Methods

  // Does the actual connecting and binds events onto the socket for handling
  // data/error/close.
  HelpEsb.Client.prototype._connect = function(uri) {
    var uriObj = url.parse(uri);
    // This uses the basic socket connection to the ESB.  We are forcing utf-8
    // here as we shouldn't really use anything else.
    this._socket = Promise.promisifyAll(
      net.createConnection({host: uriObj.hostname, port: uriObj.port})
    );
    this._socket.setEncoding('utf-8');

    // We can't send anything over the socket until we have a connection.  We
    // immediately initiate the connection and save a promise for it so that
    // the client ensures the connection exists before trying to send data.
    this._socketConnection = this._socket.onAsync('connect');

    // Handle data coming in over the socket using our special handler.
    // Because data can come in pieces, we have to keep a data buffer so that
    // we only process complete payloads.
    this._buffer = '';
    this._socket.on('data', this._handleData.bind(this));

    // Error handling is a bit simpler - we can just pass the error to the
    // user's configured error handler.
    this._socket.on('error', this.emit.bind(this, 'type.error'));

    this._socket.on('close', this._reconnect.bind(this, uri));
  };

  // This reconnects to the ESB and sets up the resubscription handler.
  HelpEsb.Client.prototype._reconnect = function(uri) {
    var previousSubscriptions = Object.keys(this._subscriptions);

    this._socket.destroy();
    this._connect(uri);

    this._socket.on(
      'connect',
      this._resubscribe.bind(this, this._login, previousSubscriptions)
    );
  };

  // Reauthenticates and resubscribes to the socket using the given data.
  HelpEsb.Client.prototype._resubscribe = function(login, subscriptions) {
    this._subscriptions = {};

    if (login !== null) {
      this.emit('socket.reconnect');
      this.login(login);
      subscriptions.forEach(this.subscribe, this);
    }
  }

  // Format the packet for the ESB and send it over the socket.  JSON encodes
  // the message and appends a newline as the delimiter between messages.
  HelpEsb.Client.prototype._send = function(packet, replyCallback) {
    packet = this._massageOutboundPacket(packet);

    // Register a callback for replies to this message if a callback is given.
    if (replyCallback) {
      this.once('replyTo.' + packet.meta.id, _.partial(replyCallback, null));
    }

    return this._sendRaw(JSON.stringify(packet) + '\n');
  };

  // Sends the packet like **_send**, but returns a promise for a response from
  // some other service.  This uses the autogen message id and relies on the
  // other service properly publishing a message with a proper replyTo.
  HelpEsb.Client.prototype._rpcSend = function(packet) {
    var send = Promise.promisify(HelpEsb.Client.prototype._send).bind(this);
    return send(packet).spread(this._checkRpcResult);
  };

  // Checks an RPC response and fails the promise if the response is not
  // successful.
  HelpEsb.Client.prototype._checkRpcResult = function(data, meta) {
    if (meta.result !== 'SUCCESS') {
      return Promise.reject(meta.reason);
    }

    return Promise.resolve(data);
  };

  // Wait on the socket connection and once it is avaialable send the given
  // string data returning a promise of the data being sent.
  HelpEsb.Client.prototype._sendRaw = function(data) {
    return this._socketConnection.then(function() {
      return this._socket.writeAsync(data);
    }.bind(this));
  };

  // Handle an incoming slice of data over the socket.  Split the message on
  // the newline delimiters and pass each complete packet to `_handlePacket`.
  HelpEsb.Client.prototype._handleData = function(data) {
    // Continue to append to the buffer.  The full message may not come in one
    // piece.
    this._buffer += data;
    if (this._buffer.indexOf('\n') !== -1) {
      // It is even possible that multiple packets were sent at once and so we
      // need to make sure we parse all of them.
      var packets = this._buffer.split('\n');

      // The section after the last newline (which may be empty) is kept in the
      // buffer as it belongs to the next packet.
      this._buffer = packets[packets.length - 1];

      packets.slice(0, -1).forEach(this._handlePacket.bind(this));
    }
  };

  // Handles a single packet of data.  The data is expected to be JSON, and if
  // it isn't, a `type.error` event will be emitted.  Otherwise, an event for
  // each of the meta fields (e.g., `type.error`, `group.someGroup`,
  // `replyTo.SOME_ID`) will be emitted.
  //
  // In addition, non-error packets will be emitted to the `*` event and, if no
  // listeners were fired for the packet, to the `*.unhandled` event.
  //
  // In the future, this will also be responsible for handling "special"
  // packets like heartbeats, etc. that are kept separate from the primary
  // payload packets.
  HelpEsb.Client.prototype._handlePacket = function(packet) {
    try {
      packet = JSON.parse(packet);
    } catch (e) {
      this.emit('type.error', e);
      return;
    }

    if (
      typeof packet.meta !== 'object' ||
      typeof packet.meta.type !== 'string' ||
      typeof packet.data === 'undefined'
    ) {
      this.emit('type.error', 'Invalid format detected for packet', packet);
      return;
    }

    var handled = _.map(packet.meta, function(value, key) {
      return this.emit(key + '.' + value, packet.data, packet.meta);
    }.bind(this));

    this.emit('*', packet.data, packet.meta);
    if (!_.any(handled)) {
      this.emit('*.unhandled', packet.data, packet.meta);
    }
  };

  // Process the packet to ensure it conforms to the ESB requirements.  Sets
  // the message id in the metadata for the packet if it wasn't already set.
  HelpEsb.Client.prototype._massageOutboundPacket = function(packet) {
    packet.meta.id = packet.meta.id || uuid.v4();

    return packet;
  };

  return HelpEsb;
}));
