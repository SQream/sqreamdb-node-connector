"use strict";
const ParseSqreamData = require('./ParseSqreamData');

module.exports = function HandleBuffer() {
// const handleBuffer = {*/
    var parseData = null;
    var varchar_encoding = "ascii"
    /*writeBuffer: function(data) {
     var buf = new Buffer.alloc(10 + data.length);
     buf.writeInt8(4, 0);
     buf.writeInt8(1, 1);
     buf.writeUInt32LE(data.length, 2); //write the high order bits (shifted over)
     buf.write(data, 10);
     return buf;
     },*/

    function set_varchar_encoding(encoding) {

        varchar_encoding = encoding
    } 

    function readBuffer(buf, dir, datafiles) {
        if (buf.length < 10) {
            return 'incomplete buffer';
        }

        try {
            var msgSize = buf.readUInt32LE(2);
            const message = buf.slice(10, msgSize + 10);
            if (message.length < msgSize) {
                return 'incomplete buffer';
            }

            const msgJson = JSON.parse(message.toString());
            if (buf.includes('queryTypeNamed', 10)) {
                this.parseData = new ParseSqreamData();
                this.parseData.setColumnsType(msgJson, dir, datafiles);
                this.parseData.set_varchar_encoding(varchar_encoding)
            }

            if (buf.includes('colSzs', 10)) {
                if (msgJson.rows > 0) {
                    var fetchBuff = buf.slice(10 + msgSize);
                    if (fetchBuff.length < 10) {
                        return 'incomplete buffer';
                    }
                    const fetchSize = fetchBuff.readUInt32LE(2);
                    const fetchBuffMessage = fetchBuff.slice(10, fetchSize + 10);
                    if (fetchBuffMessage.length < fetchSize) {
                        return 'incomplete buffer';
                    }

                    this.parseData.setColSzs(msgJson);
                    msgJson.data = this.parseData.fetchData(fetchBuff, dir, datafiles);
                } else {
                    return {done: true};
                }
            }
            return msgJson;
        } catch (ex) {
            return 'incomplete buffer';
        }

    }

    function jsonToBuffer(data) {
        const dataString = JSON.stringify(data);
        var dataLength = dataString.length;

        var buf = new Buffer.alloc(10 + dataLength);
        buf.writeInt8(4, 0);
        buf.writeInt8(1, 1);

        buf.writeUInt32LE(dataLength, 2); //write the high order bits (shifted over)

        buf.write(dataString, 10, dataLength, 'utf8');

        return buf;
    }

    function jsonToBufferWithUTF(data) {
        const regex = /[^\u0000-\u007F]+/g;

        const dataString = JSON.stringify(data);
        var dataLength = dataString.length;
        const stringWithoutUnicode = dataString.replace(regex, '');

        if (stringWithoutUnicode.length !== dataLength) {
            const nonAsciiDiffLength = dataLength - stringWithoutUnicode.length;
            dataLength += nonAsciiDiffLength * 3;
        }


        var buf = new Buffer.alloc(10 + dataLength);
        buf.writeInt8(4, 0);
        buf.writeInt8(1, 1);

        buf.writeUInt32LE(dataLength, 2); //write the high order bits (shifted over)

        buf.write(dataString, 10, dataLength, 'utf8');

        //var test_buff = new Buffer();

        return buf;
    }

    function getProtocolVersion(buf) {
        return buf.readUIntBE(0, 1);
    }

    return {
        readBuffer: readBuffer,
        getProtocolVersion: getProtocolVersion,
        jsonToBuffer: jsonToBuffer,
        jsonToBufferWithUTF: jsonToBufferWithUTF,
        set_varchar_encoding: set_varchar_encoding
    };
};
