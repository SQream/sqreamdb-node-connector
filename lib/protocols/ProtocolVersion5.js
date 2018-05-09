"use strict";

module.exports = function ProtocolVersions() {
    function version5(client, self, response, sql, callback) {
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
                msg = {"getStatementId": "getStatementId"};
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

                msg = {"chunkSize": 0, "prepareStatement": sql};
                buffer = self.handleMessage.jsonToBufferWithUTF(msg);
                client.write(buffer);
                break;

            case 'statementPrepared':
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

    function setCommand5 (client, self, response, setCommand) {
        var msg = {};
        var buffer;
        const keys = Object.keys(response);
        switch (keys[0]) {
            case 'connectionId':
            case 'databaseConnected':
                msg = {"getStatementId": "getStatementId"};
                buffer = self.handleMessage.jsonToBuffer(msg);
                client.write(buffer);
                break;

            case 'statementId':
                msg = {"chunkSize": 0, "prepareStatement": setCommand};
                buffer = self.handleMessage.jsonToBufferWithUTF(msg);
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

                msg = {"getStatementId": "getStatementId"};
                buffer = self.handleMessage.jsonToBuffer(msg);
                client.write(buffer);
                break;
        }
    }

    return {
        version5: version5,
        setCommand5: setCommand5
    };
};
