"use strict";

module.exports = function ProtocolVersions() {

    function version4(client, self, response, sql, callback) {
        var msg = {};
        var buffer;
        const keys = Object.keys(response);
        switch (keys[0]) {
            case 'connectionId':
            case 'databaseConnected':
                self.events.emit('getConnectionId', {
                    host: self.host,
                    port: self.port,
                    connectionId: response.connectionId
                });
                msg = {"chunkSize": 0, "prepareStatement": sql};
                buffer = self.handleMessage.jsonToBufferWithUTF(msg);
                client.write(buffer);
                break;

            case 'statementPrepared':
                const outData = {
                    host: self.host,
                    port: self.port,
                    statement_id: response.statement_id
                };
                self.events.emit('getStatementId', outData);
                msg = {"queryTypeOut": "queryTypeOut"};
                buffer = self.handleMessage.jsonToBuffer(msg);
                client.write(buffer);
                break;

            case 'queryTypeNamed':
                self.queryTypeNamed = response.queryTypeNamed;
                self.events.emit('getTypes', response.queryTypeNamed);
                msg = {"execute": "execute"};
                buffer = self.handleMessage.jsonToBuffer(msg);
                client.write(buffer);
                break;
            case 'executed':
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
    }

    function setCommand4 (client, self, response, setCommand, sql) {
        var msg = {};
        var buffer;
        const keys = Object.keys(response);
        // console.log(response);
        switch (keys[0]) {
            case 'connectionId':
            case 'databaseConnected':
                var msg = {"chunkSize": 0, "prepareStatement": setCommand};
                buffer = self.handleMessage.jsonToBuffer(msg);
                client.write(buffer);
                break;

            case 'statementPrepared':
                msg = {"execute": "execute"};
                buffer = self.handleMessage.jsonToBufferWithUTF(msg);
                client.write(buffer);
                break;

            case 'executed':
                msg = {"closeStatement": "closeStatement"};
                buffer = self.handleMessage.jsonToBuffer(msg);
                client.write(buffer);
                break;

            case 'statementClosed':
                self.setCommandRun = true;
                var msg = {"chunkSize": 0, "prepareStatement": sql};
                buffer = self.handleMessage.jsonToBuffer(msg);
                client.write(buffer);
                break;
        }
    }

    return {
        version4: version4,
        setCommand4: setCommand4
    };
};

