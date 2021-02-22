"use strict";
const clientProtocolVersion = 6;
const parser = require('./SqreamDataParser');
const ParseSqreamData = parser.ParseSqreamData;
const createInsertBuffer = require('./SqreamDataWriter').createInsertBuffer;

function parseColsData(fetchBuff, maxRows, varchar_encoding, colTypesMsg, colSzsMsg) {
  const parseData = new ParseSqreamData();
  parseData.setVarcharEncoding(varchar_encoding);
  const res = parseData.fetchData(fetchBuff, maxRows, colSzsMsg, colTypesMsg);
  return res;
}

function jsonToBuffer(data, encoding = 'utf8') {
  const dataString = JSON.stringify(data);
  var dataLength = Buffer.byteLength(dataString, encoding);

  var buf = new Buffer.alloc(10 + dataLength);
  buf.writeInt8(clientProtocolVersion, 0);
  buf.writeInt8(1, 1);

  buf.writeUInt32LE(dataLength, 2); //write the high order bits (shifted over)

  buf.write(dataString, 10, dataLength, encoding);

  return buf;
}


function messageHeader(dataLength, isBinary) {
  const buf = new Buffer.alloc(10);
  buf.writeInt8(clientProtocolVersion, 0);
  buf.writeInt8(isBinary ? 2 : 1, 1);
  buf.writeUInt32LE(dataLength, 2); //write the high order bits (shifted over)
  return buf;
}

function jsonToBufferWithUTF(data) {
  return jsonToBuffer(data,'utf8');
}

function getProtocolVersion(buf) {
  return buf.readUIntBE(0, 1);
}

module.exports = {
  getProtocolVersion: getProtocolVersion,
  jsonToBuffer: jsonToBuffer,
  jsonToBufferWithUTF: jsonToBufferWithUTF,
  parseColsData: parseColsData,
  createInsertBuffer: createInsertBuffer,
  clientProtocolVersion,
  messageHeader
};
