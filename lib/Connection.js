"use strict";
const EventEmitter = require('events');
const net = require('net');
const tls = require('tls');
const path = require('path');
const HandleMessage = require('./HandleMessage');

class Connection  {
  constructor(config) {
    this.config = config;
    this.data = [];
    this.currentBuffer = null;
    this.isIncompleteBuffer = false;

    this.events = new EventEmitter();
    this.handleMessage = new HandleMessage();
    this.version = null;
    this.protocol = null;
    this.statementClose = false;
    this.setCommandRun = false;
  }

  dynamicLoadProtocol (version) {
    const file = path.join(__dirname,'protocols', 'ProtocolVersion'+version+'.js');
    const protocol = require(file);
    this.protocol = new protocol();
  }

  connectCluster(sql, callback, setCommand) {
    const self = this;
    const config = this.config;
    const client = new net.Socket();

    client.connect({port: config.port, host: config.host}, function() {
      const cred = {
        username: config.username,
        password: config.password,
        connectDatabase: config.connectDatabase};

      const buffer = self.handleMessage.jsonToBuffer(cred);
      client.write(buffer);
    });

    client.on('data', function(buf) {
      const ipSize =  buf.readInt8(0);
      const ip = buf.slice(4, 4 + ipSize).toString('ascii');
      const port = buf.readInt16LE(4 + ipSize);

      self.executeQuery(sql, callback, setCommand, ip, port);
      client.destroy();
    });

    client.on('error', function(err) {
      console.log("error", err);
      callback(err)
    });

    client.on('close', function() {
      // console.log("close");
    });
  }

  executeQuery(sql, callback, setCommand, hostArg, portArg) {
    const self = this;
    const config = this.config;
    const client = config.is_ssl ? new tls.TLSSocket() : new net.Socket();
    // const client = new tls.TLSSocket();
    const host = hostArg || config.host;
    const port = portArg || config.port;
    // var setCommandRun = false;
    self.host = host;
    self.port = port;
    // const shortSQl = sql.substring(0, 70);

    client.connect({port: port, host: host}, function() {
      const cred = {
        username: config.username,
        password: config.password,
        connectDatabase: config.connectDatabase};

      const buffer = self.handleMessage.jsonToBuffer(cred);

      client.write(buffer);
    });

    client.on('data', function(data) {
      if (!this.version) {
          this.version = self.handleMessage.getProtocolVersion(data);
          self.dynamicLoadProtocol(this.version);
      }

      if (this.isIncompleteBuffer) {
        data = Buffer.concat([this.currentBuffer, data]);
        this.isIncompleteBuffer = false;
      }
      this.currentBuffer = data;

      const response = self.handleMessage.readBuffer(data);

      if (response == 'incomplete buffer') {
        this.isIncompleteBuffer = true;
        return;
      }
      if (response.data) {
        self.data = self.data.concat(response.data);
      }

      if (setCommand && !self.setCommandRun ) {
          self.protocol['setCommand'+this.version](client, self,response, setCommand, sql);
        return;
      }

      self.protocol['version'+this.version](client, self, response, sql, callback);
    });

    client.on('close', function() {
      if (!self.statementClose) {
        callback('The server unexpectedly closed the connection before the statement could finish. Please verify that the SQream DB server is running and try again. If this problem persists, please contact SQream support.');
      }
    });

    client.on('error', function(err) {
      // Fire error then not need to error on Close event.
      self.statementClose = true;
      callback(err);
    });
  }

  runQuery (sql, callback, setCommand) {
    this.sql = sql;
    if (this.config.cluster) {
      this.connectCluster(sql, callback, setCommand);
    } else {
      this.executeQuery(sql, callback, setCommand);
    }
  }
} //EOC

module.exports = Connection;
