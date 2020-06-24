"use strict";
const iconv = require('iconv-lite');

module.exports = function ParseSqreamData() {

  let varchar_encoding = 'ascii';
  function setVarcharEncoding(encoding) {
    varchar_encoding = encoding || 'ascii';
  }

  // As implememnted in node 12
  // https://github.com/nodejs/node/blob/ed8fc7e11d688cbcdf33d0d149830064758bdcd2/lib/internal/buffer.js
  function readBigInt64LE(buffer, offset = 0) {
    const first = buffer[offset];
    const last = buffer[offset + 7];
    if (first === undefined || last === undefined)
      boundsError(offset, buffer.length - 8);
    const val = buffer[offset + 4] +
      buffer[offset + 5] * 2 ** 8 +
      buffer[offset + 6] * 2 ** 16 +
      (last << 24); // Overflow
    return (BigInt(val) << 32n) +
      BigInt(first +
      buffer[++offset] * 2 ** 8 +
      buffer[++offset] * 2 ** 16 +
      buffer[++offset] * 2 ** 24);
  }

  let currentVarcharPosition = 0;

  const fetchBufferByColType = {
    ftLong: function (dataBuffer, fieldSize, i) {
       return readBigInt64LE(dataBuffer, i * fieldSize);
       // return dataBuffer.readBigInt64LE(i * fieldSize); // when we move to node 12
    },
    ftVarchar: function (dataBuffer, fieldSize, i) {
      const varCharPosition = i * fieldSize;
      return iconv.decode(dataBuffer.slice(varCharPosition, varCharPosition + fieldSize), varchar_encoding).replace(/\0/g, '').trimEnd();
    },
    ftInt: function (dataBuffer, fieldSize, i) {
      return dataBuffer.readInt32LE(i * fieldSize);
    },
    ftDouble: function (dataBuffer, fieldSize, i) {
      return dataBuffer.readDoubleLE(i * fieldSize);
    },
    ftFloat: function (dataBuffer, fieldSize, i) {
      return dataBuffer.readFloatLE(i * fieldSize);
    },
    ftBool: function (dataBuffer, fieldSize, i) {
      const varCharPosition = i * fieldSize;
      return dataBuffer.slice(varCharPosition, varCharPosition + 1)[0];
    },
    ftDate: function (dataBuffer, fieldSize, i) {
      const dateNum = dataBuffer.readInt32LE(i * fieldSize);
      return parseSqreamDate(dateNum);
    },
    ftDateTime: function (dataBuffer, fieldSize, i) {
      return parseSQreamDataTime(dataBuffer, fieldSize, i)
    },
    ftBlob: function (dataBuffer, fieldSize, i, varcharBuffer) {
      const itemSize = varcharBuffer.readInt32LE(i * 4);
      const value  = dataBuffer.slice(currentVarcharPosition, currentVarcharPosition + itemSize).toString('utf8');
      currentVarcharPosition += itemSize;
      return value ;
    },
    ftUByte: function (dataBuffer, fieldSize, i) {
      return dataBuffer.readUIntLE(fieldSize * i, fieldSize);
    },
    ftShort: function (dataBuffer, fieldSize, i) {
      return dataBuffer.readInt16LE(fieldSize * i, fieldSize);
    }
  };

  function parseSqreamDate(dateNum) {
    var year = Math.floor(Math.floor((10000 * dateNum + 14780)) / 3652425);
    const xx = Math.floor(365 * year + Math.floor(year / 4) - Math.floor(year / 100) + Math.floor(year / 400));
    var ddd = Math.floor(dateNum - xx);

    if (ddd < 0) {
      year = year - 1;
      ddd = Math.floor((dateNum - (365 * year + Math.floor(year / 4) - Math.floor(year / 100) + Math.floor(year / 400))));
    }

    var mi = Math.floor(Math.floor((100 * ddd + 52)) / 3060);
    var month = ((mi + 2) % 12) + 1;
    year = Math.floor(year + (mi + 2) / 12);
    var day = ddd - Math.floor((mi * 306 + 5) / 10) + 1;

    year += "";
    month += "";
    day += "";

    month = month.padStart(2,'0');
    day = day.padStart(2,'0');

    return [year, month, day].join("-");
  }
  function parseSQreamDataTime(dataBuffer, fieldSize, i){
    var dateNum = dataBuffer.readUInt32LE(i * fieldSize + 4);
    var timeNum = dataBuffer.readUInt32LE(i * fieldSize);
    var dateSql = parseSqreamDate(dateNum);

    var timeHelper = timeNum;
    var hour = Math.floor(timeNum / 1000 / 60 / 60);
    timeHelper = Math.floor(timeHelper - (hour * 60 * 60 * 1000));
    var minutes = Math.floor(timeHelper / 1000 / 60);
    timeHelper = Math.floor(timeHelper - (minutes * 60 * 1000));
    var secs = Math.floor((timeHelper) / 1000);
    timeHelper = Math.floor(timeHelper - (secs * 1000));
    var milis = timeHelper;

    hour += "";
    minutes += "";
    secs += "";
    milis += "";

    hour = hour.padStart(2,'0');
    minutes = minutes.padStart(2,'0');
    secs = secs.padStart(2,'0');
    milis = milis.padStart(3,'0');

    return [dateSql, ' ', hour, ':', minutes, ':', secs, '.', milis].join("");
  }

  function fetchData(buf, maxRows, colSzs, columnsType) {
    var rows = [];

    const queryTypeNamedLenght = columnsType.queryTypeNamed.length;
    const numberOfRows = Math.min(colSzs.rows, maxRows);
    let currentField;
    let firstBufferSize = 0;
    let currentOffSet = 10;
    let value;

    //prapare rowData ahead
    rows.length = numberOfRows;
    const template = {};
    columnsType.queryTypeNamed.forEach((col) => template[col.name] = undefined);
    for (let i = 0; i < numberOfRows; i++) {
      rows[i] = {...template};
    }

    function getBufferSize() {
      const size = colSzs.colSzs[firstBufferSize];
      firstBufferSize++;
      return size;
    }

    function getChunkBuffer(size){
      const chunk = buf.slice(currentOffSet, currentOffSet+size);
      currentOffSet+=size;
      return chunk;
    }

    for(var colTypes = 0; colTypes < queryTypeNamedLenght ; colTypes++) {
      currentVarcharPosition = 0;
      currentField = columnsType.queryTypeNamed[colTypes];

      var nullBufferSize;
      var varcharSizeBuffer;
      var dataBufferSize;
      var varcharBuffer;

      if(currentField.nullable) {
        nullBufferSize = getBufferSize();
        var nullBuffer = getChunkBuffer(nullBufferSize); //buf.slice(currentOffSet, currentOffSet+nullBufferSize) ;
      }

      if (currentField.isTrueVarChar){
        varcharSizeBuffer = getBufferSize();
        varcharBuffer = getChunkBuffer(varcharSizeBuffer);
      }

      dataBufferSize = getBufferSize();
      var dataBuffer = getChunkBuffer(dataBufferSize);
      var fieldType = currentField.type[0];
      var fieldSize = currentField.type[1];
      
      const fieldCb = this.fetchBufferByColType[fieldType];

      for (let i = 0; i < numberOfRows; i++) {
        if(currentField.nullable) {
          if (nullBuffer[i]) {
            value = null;
          } else {
            value = fieldCb(dataBuffer,fieldSize, i, varcharBuffer);
          }
        } else {
          value = fieldCb(dataBuffer,fieldSize, i, varcharBuffer);
        }
        rows[i][currentField.name] = value;
      }
    }

    return rows;
  }

  return {
    fetchBufferByColType: fetchBufferByColType,
    fetchData: fetchData,
    setVarcharEncoding: setVarcharEncoding 
  };
};
