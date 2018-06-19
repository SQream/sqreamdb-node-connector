"use strict";
const EventEmitter = require('events');
const net = require('net');
const tls = require('tls');
const path = require('path');
const HandleMessage = require('./HandleMessage');

class Connection {
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
        this.statement_id = null;
        this.connectionId = null;
    }

    connectCluster(sql, callback, setCommand) {
        const self = this;
        const config = this.config;
        const client = new net.Socket();

        client.connect({port: config.port, host: config.host}, function () {
            const cred = {
                username: config.username,
                password: config.password,
                connectDatabase: config.connectDatabase
            };

            const buffer = self.handleMessage.jsonToBuffer(cred);
            client.write(buffer);
        });

        client.on('data', function (buf) {
            const ipSize = buf.readInt8(0);
            const ip = buf.slice(4, 4 + ipSize).toString('ascii');
            const port = buf.readInt16LE(4 + ipSize);

            self.executeQuery(sql, callback, setCommand, ip, port);
            client.destroy();
        });

        client.on('error', function (err) {
            console.log("error", err);
            callback(err)
        });

        client.on('close', function () {
            // console.log("close");
        });
    }

    executeQuery(sql, callback, setCommand, hostArg, portArg) {
        const self = this;
        const config = this.config;
        const client = config.is_ssl ? new tls.TLSSocket() : new net.Socket();
        const host = hostArg || config.host;
        const port = portArg || config.port;

        self.host = host;
        self.port = port;

        client.connect({port: port, host: host}, function () {
            const cred = {
                username: config.username,
                password: config.password,
                connectDatabase: config.connectDatabase,
                service: config.service || 'sqream'
            };

            const buffer = self.handleMessage.jsonToBuffer(cred);

            client.write(buffer);
        });

        client.on('data', function (data) {
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

            const keys = Object.keys(response);

            if (setCommand && !self.setCommandRun) {
                switch (keys[0]) {
                    case 'connectionId':
                    case 'databaseConnected':
                        self.connectionId = response.connectionId;
                        msg = {"getStatementId": "getStatementId"};
                        buffer = self.handleMessage.jsonToBuffer(msg);
                        client.write(buffer);
                        break;

                    case 'statementId':
                        self.statement_id = response.statementId;
                        msg = {"chunkSize": 0, "prepareStatement": setCommand};
                        buffer = self.handleMessage.jsonToBufferWithUTF(msg);
                        client.write(buffer);
                        break;


                    case 'statementPrepared':
                        msg = {"execute": "execute"};
                        buffer = self.handleMessage.jsonToBufferWithUTF(msg);
                        client.write(buffer);
                        break;

                    case 'ip':
                        msg = {
                            username: config.username,
                            password: config.password,
                            db: config.connectDatabase,
                            service: config.service || 'sqream',
                            statement_id: self.statement_id,
                            execute: 0
                        };


                        buffer = self.handleMessage.jsonToBuffer(msg);
                        client.write(buffer);
                        break;

                    case 'executed':
                        msg = {"closeStatement": "closeStatement"};
                        buffer = self.handleMessage.jsonToBuffer(msg);
                        client.write(buffer);
                        break;

                    case 'statementClosed':
                        self.setCommandRun = true;

                        msg = {"getStatementId": "getStatementId"};
                        buffer = self.handleMessage.jsonToBuffer(msg);
                        client.write(buffer);
                        break;
                }

                return;
            }


            var msg = {};
            var buffer;

            switch (keys[0]) {
                case 'connectionId':
                case 'databaseConnected':
                    if (!self.connectionId) {
                        self.events.emit('getConnectionId', {
                            host: self.host,
                            port: self.port,
                            connectionId: response.connectionId
                        });
                        self.connectionId = response.connectionId;
                        msg = {"getStatementId": "getStatementId"};
                    } else {
                        msg = {"reconstructStatement": self.statement_id};
                    }

                    buffer = self.handleMessage.jsonToBufferWithUTF(msg);
                    client.write(buffer);

                    break;

                case 'statementId':
                    const outData = {
                        host: self.host,
                        port: self.port,
                        statement_id: response.statementId
                    };
                    self.events.emit('getStatementId', outData);

                    self.statement_id = response.statementId;
                    msg = {"chunkSize": 0, "prepareStatement": sql};
                    buffer = self.handleMessage.jsonToBufferWithUTF(msg);
                    client.write(buffer);
                    break;

                case 'ip':
                    self.reconnection = true;

                    if (response.reconnect) {
                        msg = {
                            reconnectDatabase: config.connectDatabase,
                            service: config.service || 'sqream',
                            connectionId: self.connectionId,
                            username: config.username,
                            password: config.password,
                            listenerId: response.listener_id
                        };
                        client.destroy();

                        client.connect({port: response.port, host: response.ip}, function () {
                            buffer = self.handleMessage.jsonToBufferWithUTF(msg);
                            client.write(buffer);
                        });
                    } else {
                        msg = {"execute": "execute"};

                        buffer = self.handleMessage.jsonToBufferWithUTF(msg);
                        client.write(buffer);
                    }

                    break;

                case 'statementReconstructed':
                    msg = {"execute": "execute"};

                    buffer = self.handleMessage.jsonToBufferWithUTF(msg);
                    client.write(buffer);
                    break;

                case 'executed':
                    msg = {"queryTypeIn": "queryTypeIn"};
                    buffer = self.handleMessage.jsonToBufferWithUTF(msg);
                    client.write(buffer);
                    break;

                case 'queryType':
                    msg = {"queryTypeOut": "queryTypeOut"};
                    buffer = self.handleMessage.jsonToBufferWithUTF(msg);
                    client.write(buffer);
                    break;


                case 'queryTypeNamed':
                    self.queryTypeNamed = response.queryTypeNamed;
                    self.events.emit('getTypes', response.queryTypeNamed);
                    if (response.queryTypeNamed.length === 0) {
                        msg = {"closeStatement": "closeStatement"};
                    } else {
                        msg = {"fetch": "fetch"};
                    }

                    buffer = self.handleMessage.jsonToBuffer(msg);
                    client.write(buffer);
                    break;

                case 'colSzs':
                    msg = {"fetch": "fetch"};
                    if (self.queryTypeNamed.length === 0) {
                        msg = {"closeStatement": "closeStatement"};
                    }
                    buffer = self.handleMessage.jsonToBuffer(msg);
                    client.write(buffer);
                    break;

                case 'done':
                    msg = {"closeStatement": "closeStatement"};
                    buffer = self.handleMessage.jsonToBuffer(msg);
                    client.write(buffer);
                    break;

                case 'statementClosed':
                    callback(null, self.data);
                    self.statementClose = true;
                    msg = {"closeConnection": "closeConnection"};
                    buffer = self.handleMessage.jsonToBuffer(msg);
                    client.write(buffer);
                    break;

                case 'connectionClosed':
                    client.destroy();
                    break;

                case 'error':
                    self.statementClose = true;
                    callback(response.error);
                    client.destroy();
                    break;
            }
        });

        client.on('close', function () {
            if (self.reconnection) {
                return;
            }
            if (!self.statementClose) {
                callback('The server unexpectedly closed the connection before the statement could finish. Please verify that the SQream DB server is running and try again. If this problem persists, please contact SQream support.');
            }
        });

        client.on('error', function (err) {
            // Fire error then not need to error on Close event.
            self.statementClose = true;
            callback(err);
        });
    }

    runQuery(sql, callback, setCommand) {
        this.sql = sql;
        if (this.config.cluster) {
            this.connectCluster(sql, callback, setCommand);
        } else {
            this.executeQuery(sql, callback, setCommand);
        }
    }
} //EOC

module.exports = Connection;
