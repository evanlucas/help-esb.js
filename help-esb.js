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
    require('lodash'),
    require('object-path')
  );
}(this, function(
  HelpEsb,
  net,
  EventEmitter,
  util,
  url,
  Promise,
  uuid,
  _,
  objectPath
) {
  'use strict';

  // ## HelpEsb.Client

  // ### HelpEsb.Client *constructor*
  // The client connects to the ESB running on the given host/port.  You will
  // need to [login](#helpesb-client-login) before doing anything over the
  // connection.
  //
  //     var client = new Esb.Client('tcp://example.com:1234');
  //     client.login('clientName');
  //     client.subscribe('subscriptionChannel1');
  //     client.on('type.error', console.error);
  //     client.on('group.subscriptionChannel1', function(message) {
  //       // Process message
  //     });
  //
  // Or using the RPC conventions:
  //
  //     var client = new Esb.Client('tcp://example.com:1234');
  //     client.login('clientName');
  //     client.rpcReceive('subscriptionChannel1', function(message) {
  //       // Process message
  //       return result;
  //     });
  HelpEsb.Client = function(uri, options) {
    // Extend EventEmitter to handle events.
    EventEmitter.call(this);

    this._connect(uri);

    // Start with no authentication and no subscriptions.
    this._authentication = null;
    this._subscriptions = {};
    this._login = null;
    this._options = _.extend({debug: false}, options);

    this.mb = new HelpEsb.MessageBuilder(this);
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

    return this._authentication = this._rpcSend(this.mb.login(name))
      .timeout(10000);
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
      this._subscriptions[group] = this._authPromise().then(function() {
        return this._rpcSend(this.mb.subscribe(group)).timeout(10000);
      }.bind(this));
    }

    return this._subscriptions[group];
  };

  // ### HelpEsb.Client.send
  // Sends a payload message to the ESB with the given message.  Returns a
  // promise that, like the [subscribe](#helpesb-client-subscribe) call, is
  // fulfilled when the message is sent, but does not indicate whether the
  // message was received by the ESB or by any subscribers.  For RPC-esque
  // behavior, use [rpcSend](#helpesb-client-rpcsend).
  //
  // Optionally, you can also pass a second message instance that indicates
  // which request message this message is in regards to.  This allows for
  // following full request cycle reporting with the ESB.  We can track the
  // requests down through multiple layers using this `inre` flag, and then the
  // responses can come back up using `replyTo` and we can reconstruct the
  // topography of the calls after the fact.
  //
  //     client.send('target', {id: 1234, message: 'Hello!'});
  HelpEsb.Client.prototype.send = function(
    group,
    message,
    inre,
    replyCallback
  ) {
    return this._authPromise().then(function() {
      return this._send(
        this.mb.send(group, this.mb.coerce(message), inre),
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
  //     client.rpcSend('foo', {name: 'John'})
  //       .then(function(response) {
  //         console.log(response.toJSON());
  //       }).catch(function(error) {
  //         console.error(error);
  //       });
  HelpEsb.Client.prototype.rpcSend = function(group, message, inre) {
    var send = Promise.promisify(HelpEsb.Client.prototype.send).bind(this);
    return this.subscribe(group + '-result').then(function() {
      return send(group, message, inre).then(this._checkRpcResult);
    }.bind(this));
  };

  // ### HelpEsb.Client.rpcReceive
  // Listen on the given group like **on**, and call the given callback with
  // any messages.  The value returned by the callback is sent to the
  // GROUPNAME-result group in reply to the incoming message.
  //
  // Automatically subscribes to the group for you if not already subscribed.
  //
  //     client.rpcReceive('foo', function(message) {
  //       return {greeting: 'Hello ' + message.get('name', 'Stranger')};
  //     });
  //
  // If the callback returns a promise, the result of the promise is sent.
  //
  //     client.rpcReceive('foo', function(message) {
  //       return request.getAsync('http://www.google.com');
  //     });
  //
  // Errors are also handled and errors are sent as the "reason" through the
  // ESB.
  //
  //     client.rpcReceive('foo', function(message) {
  //       throw new Error('Not implemented!');
  //     });
  HelpEsb.Client.prototype.rpcReceive = function(group, cb) {
    this.subscribe(group);
    this.on('group.' + group, function(message) {
      var meta = {
        type: 'sendMessage',
        replyTo: message.getMeta('id'),
        inre: message.getMeta('id')
      };

      // Link up our reply to the incoming request but on the "result" group.
      var groups = [group + '-result'];

      // CC the groups in the incoming messages CC list.
      if (message.hasMeta('cc.group')) {
        groups = groups.concat(message.getMeta('cc.group'));
      }

      if (message.hasMeta('session')) {
        meta.session = message.getMeta('session');
      }

      var sendToGroup = function(message, group) {
        return this._send(this.mb.send(group, message));
      }.bind(this);

      var sendToAll = function(message) {
        return Promise.all(groups.map(_.partial(sendToGroup, message)));
      };

      Promise.try(cb.bind({}, message)).then(function(message) {
        return sendToAll(
          this.mb.success(
            this.mb.extend({meta: meta}, this.mb.coerce(message))
          )
        );
      }.bind(this)).catch(function(error) {
        var reason = error instanceof Error ? error.toString() : error;
        var errorMeta = _.extend({reason: reason}, meta);

        return sendToAll(this.mb.failure({meta: errorMeta}));
      }.bind(this));
    }.bind(this));
  };

  // ### HelpEsb.Client.close
  // Closes the connection, ending communication.
  HelpEsb.Client.prototype.close = function() {
    this.emit('socket.close');
    this._socket.removeAllListeners('close');
    this._socket.end();
  };

  // ### HelpEsb.Client.decorateMessage
  // Formats the message with client-specific values needed by the ESB.  This
  // includes the `from` key in the meta to reference the logged in client
  // channel id.
  //
  // You probably don't need to call this yourself as it is called by the
  // `MessageBuilder`.
  HelpEsb.Client.prototype.decorateMessage = function(message) {
    if (
      this._authentication !== null &&
      this._authentication.isFulfilled() &&
      this._authentication.value().has('channelId')
    ) {
      message.meta.from = this._authentication.value().get('channelId');
    }

    return message;
  };

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
    this._authentication = null;
    this._subscriptions = {};

    if (login !== null) {
      this.emit('socket.reconnect');
      this.login(login);
      subscriptions.forEach(this.subscribe, this);
    }
  };

  // Format the message for the ESB and send it over the socket.  JSON encodes
  // the message and appends a newline as the delimiter between messages.
  HelpEsb.Client.prototype._send = function(message, replyCallback) {
    // Register a callback for replies to this message if a callback is given.
    if (replyCallback) {
      this.once(
        'replyTo.' + message.getMeta('id'),
        _.partial(replyCallback, null)
      );
    }

    return this._sendRaw(JSON.stringify(message) + '\n');
  };

  // Sends the message like **_send**, but returns a promise for a response
  // from some other service.  This uses the autogen message id and relies on
  // the other service properly publishing a message with a proper replyTo.
  HelpEsb.Client.prototype._rpcSend = function(message) {
    var send = Promise.promisify(HelpEsb.Client.prototype._send).bind(this);
    return send(message).then(this._checkRpcResult);
  };

  // Checks an RPC response and fails the promise if the response is not
  // successful.
  HelpEsb.Client.prototype._checkRpcResult = function(message) {
    if (message.getMeta('result') !== 'SUCCESS') {
      return Promise.reject(message.getMeta('reason'));
    }

    return Promise.resolve(message);
  };

  // Wait on the socket connection and once it is avaialable send the given
  // string packet returning a promise of the packet being sent.
  HelpEsb.Client.prototype._sendRaw = function(packet) {
    if (this._options.debug) {
      console.log('help-esb SENDING', packet);
    }

    return this._socketConnection.then(function() {
      return this._socket.writeAsync(packet);
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

  // Handles a single packet of data.  The packet is expected to be JSON, and
  // if it isn't, a `type.error` event will be emitted.  Otherwise, an event
  // for each of the meta fields (e.g., `type.error`, `group.someGroup`,
  // `replyTo.SOME_ID`) will be emitted.
  //
  // In addition, non-error packets will be emitted to the `*` event and, if no
  // listeners were fired for the packet, to the `*.unhandled` event.
  //
  // In the future, this will also be responsible for handling "special"
  // packets like heartbeats, etc. that are kept separate from the primary
  // payload packets.
  HelpEsb.Client.prototype._handlePacket = function(packet) {
    if (this._options.debug) {
      console.log('help-esb RECEIVED', packet);
    }

    var message;

    try {
      message = new HelpEsb.Message(JSON.parse(packet));
      if (!message.hasMeta('type')) {
        throw new Error('Invalid format detected for packet');
      }
    } catch (e) {
      this.emit('type.error', e);
      return;
    }

    // Emits key.value events with the message.  If the value is an
    // array, it iterates over the array and emits events on each value in the
    // array.  Returns true if any of the events were handled.
    var emitKeyValue = function(value, key) {
      if (_.isArray(value)) {
        return _.any(_.map(value, function(valueInner) {
          return emitKeyValue(valueInner, key);
        }));
      }

      return this.emit(key + '.' + value, message);
    }.bind(this);

    this.emit('*', message);
    if (!_.any(_.map(message.getMeta(), emitKeyValue))) {
      this.emit('*.unhandled', message);
    }
  };

  // This will return a failed promise if authentication hasn't been attempted
  // yet.
  HelpEsb.Client.prototype._authPromise = function() {
    return this._authentication ||
      Promise.reject('Attempted to send data through the ESB before authenticating');
  };

  // ## HelpEsb.MessageBuilder
  // The `MessageBuilder` is a helper object that can build a `HelpEsb.Message`
  // according to standard message types.

  // ### HelpEsb.MessageBuilder *constructor*
  // Initializes the object with access to the `HelpEsb.Client` instance used
  // to decorate messages further.
  HelpEsb.MessageBuilder = function(client) {
    this._client = client;
  };

  // ### HelpEsb.MessageBuilder.login
  // Creates a standard login message for the given client name.
  HelpEsb.MessageBuilder.prototype.login = function(name) {
    return new HelpEsb.Message({
      meta: {type: 'login'},
      data: {name: name, subscriptions: []}
    });
  };

  // ### HelpEsb.MessageBuilder.subscribe
  // Creates a standard subscribe message for the given group name.
  HelpEsb.MessageBuilder.prototype.subscribe = function(group) {
    return this.create({meta: {type: 'subscribe'}, data: {channel: group}});
  };

  // ### HelpEsb.MessageBuilder.send
  // Creates a standard `sendMessage` message, extending off of the given
  // message.
  HelpEsb.MessageBuilder.prototype.send = function(group, message, inre) {
    return this.extend(
      {
        meta: {
          type: 'sendMessage',
          group: group,
          inre: inre && inre.getMeta('id')
        }
      },
      message
    );
  };

  // ### HelpEsb.MessageBuilder.success
  // Creates a standard success message which has a result status, extending
  // off of the given message.
  HelpEsb.MessageBuilder.prototype.success = function(message) {
    return this.extend({meta: {result: 'SUCCESS'}}, message);
  };

  // ### HelpEsb.MessageBuilder.failure
  // Creates a standard failure message which has a result status, extending
  // off of the given message.
  HelpEsb.MessageBuilder.prototype.failure = function(message) {
    return this.extend({meta: {result: 'FAILURE'}}, message);
  };

  // ### HelpEsb.MessageBuilder.create
  // Creates a `HelpEsb.Message` object that has been decorated by the client.
  HelpEsb.MessageBuilder.prototype.create = function(message) {
    return new HelpEsb.Message(this._client.decorateMessage(message));
  };

  // ### HelpEsb.MessageBuilder.build
  // Creates a `HelpEsb.Message` object that has been decorated by the client
  // from its constituent data and meta parameters.
  HelpEsb.MessageBuilder.prototype.build = function(data, meta) {
    return this.create({meta: meta || {}, data: data || {}});
  };

  // ### HelpEsb.MessageBuilder.coerce
  // Coerces the passed argument into a message, returning it as is if it is a
  // `HelpEsb.Message` object, or building it from its `data` (and optional
  // `meta`) otherwise.
  HelpEsb.MessageBuilder.prototype.coerce = function(data, meta) {
    return data instanceof HelpEsb.Message ? data : this.build(data, meta);
  };

  // ### HelpEsb.MessageBuilder.extend
  // Extends a message (`Message` object or POJO) with other message(s).
  // Returns a new message that is the combined data/meta parts from all of the
  // passed message arguments.
  //
  // The `data` extension has some special handling for arrays and other
  // non-object data types.  If any of the messages have an array `data` field,
  // then array concatentation is used to merge the messages together.  If any
  // of the messages have other non-object `data` fields, then order-based
  // precedence (last one wins) is used to return the `data` field unmodified
  // from the last message with one.  For objects, standard _.extend behavior
  // is used to merge the objects together.
  HelpEsb.MessageBuilder.prototype.extend = function(/* object, extension */) {
    var params = _.map(arguments, function(arg) {
      return _.clone(arg instanceof HelpEsb.Message ? arg.toJSON() : arg);
    });

    var newMeta = _.extend.apply({}, [{}].concat(_.pluck(params, 'meta')));
    var data = _.reject(_.pluck(params, 'data'), _.isUndefined);
    var arrayData = _.filter(data, _.isArray);
    if (!_.isEmpty(arrayData)) {
      return this.build(Array.prototype.concat.apply([], arrayData), newMeta);
    }

    var nonObjectData = _.reject(data, _.isObject);
    if (!_.isEmpty(nonObjectData)) {
      return this.build(_.last(nonObjectData), newMeta);
    }

    return this.build(_.extend.apply({}, [{}].concat(data)), newMeta);
  };

  // ## HelpEsb.Message
  // A data object representing an ESB message.  Also provides some convenience
  // methods.

  // ### HelpEsb.Message *constructor*
  // Initiates the message based on the given message object.  Initializes the
  // meta and data fields appropriately, including adding a message id if one
  // does not exist.
  HelpEsb.Message = function(message) {
    this._data = _.has(message, 'data') ? message.data : {};
    this._meta = _.extend({id: uuid.v4()}, message.meta);
  };

  // ### HelpEsb.Message.get
  // Get the data property with the given dot-delimited path.  For example,
  //
  //     message = new HelpEsb.Message({foo: {bar: 'baz'}});
  //     message.get('foo.bar') === 'baz';
  //
  // You can also provide a default value to return instead of `undefined` for
  // values that don't exist.
  HelpEsb.Message.prototype.get = function(path, def) {
    return objectPath.get(this._data, path, def);
  };

  // ### HelpEsb.Message.getMeta
  // Like [get](#helpesb-message-get), but for the meta fields.
  HelpEsb.Message.prototype.getMeta = function(path, def) {
    return objectPath.get(this._meta, path, def);
  };

  // ### HelpEsb.Message.has
  // Checks for the existence of the data proper with the given dot-delimited
  // path.
  HelpEsb.Message.prototype.has = function(path) {
    return objectPath.has(this._data, path);
  };

  // ### HelpEsb.Message.hasMeta
  // Like [has](#helpesb-message-has), but for the meta fields.
  HelpEsb.Message.prototype.hasMeta = function(path) {
    return objectPath.has(this._meta, path);
  };

  // ### HelpEsb.Message.toJSON
  // Converts the message into its canonical form for JSON serialization.
  HelpEsb.Message.prototype.toJSON = function() {
    return {meta: this._meta, data: this._data};
  };

  return HelpEsb;
}));
