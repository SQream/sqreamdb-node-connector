"use strict";
const iconv = require('iconv-lite');
// const sqreamCppBuffer = require('./addon/sqreamLongAddon.node');
// The BigInt addon has to be compiled for every version of node. This imports the correct one
// const sqreamCppBuffer = (parseFloat(process.version.slice(1,)) >= 10) ? require('./addon/sqreamLongAddon.node') : require('./addon/sqreamLongAddon_8.node')
var varchar_encoding = "ascii"

module.exports = function ParseSqreamData() {
  function setColumnsType(data) {
    this.columnsType = data;
  }

  function setColSzs(data) {
    this.colSzs = data;
  }

  function set_varchar_encoding(encoding) {
    varchar_encoding = encoding || 'ascii';
  }

  function readBigUInt64LE(buffer, offset = 0) {
    const first = buffer[offset];
    const last = buffer[offset + 7];
    if (first === undefined || last === undefined) {
      throw new Error('Out of bounds');
    }
    const lo = first +
      buffer[++offset] * 2 ** 8 +
      buffer[++offset] * 2 ** 16 +
      buffer[++offset] * 2 ** 24;
      console.log("lo:", lo)
    const hi = buffer[++offset] +
      buffer[++offset] * 2 ** 8 +
      buffer[++offset] * 2 ** 16 +
      last * 2 ** 24;

    return BigInt(lo) + (BigInt(hi) << 32n);
  }

  const fetchBufferByColType = {
   currentVarcharPosition: 0,
    ftLong: function (dataBuffer, fieldSize, i) {
       return readBigUInt64LE(dataBuffer, i * fieldSize);
    },
    ftVarchar: function (dataBuffer, fieldSize, i) {
      const varCharPosition = i * fieldSize;
      return iconv.decode(dataBuffer.slice(varCharPosition, varCharPosition + fieldSize), varchar_encoding);
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
      const value  = dataBuffer.slice(this.currentVarcharPosition, this.currentVarcharPosition + itemSize).toString('utf8');
      this.currentVarcharPosition+=itemSize;
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
    month = ('0' + month).slice(-2);
    year = Math.floor(year + (mi + 2) / 12);
    var day = ddd - Math.floor((mi * 306 + 5) / 10) + 1;
    day = ('0' + day).slice(-2);

    return `${year}-${month}-${day}`;
  }

  function parseSQreamDataTime(dataBuffer, fieldSize, i){
    var dateNum = dataBuffer.readUInt32LE(i * fieldSize + 4);
    var timeNum = dataBuffer.readUInt32LE(i * fieldSize);
    var dateSql = parseSqreamDate(dateNum);

    function shiftZero(value) {
      return ('0' + value).slice(-2);
    }

    function shiftMili(value) {
      return value.toString().padStart(3,'0');
    }

    var timeHelper = timeNum;
    var hour = Math.floor(timeNum / 1000 / 60 / 60);
    timeHelper = Math.floor(timeHelper - (hour * 60 * 60 * 1000));
    var minutes = Math.floor(timeHelper / 1000 / 60);
    timeHelper = Math.floor(timeHelper - (minutes * 60 * 1000));
    var secs = Math.floor((timeHelper) / 1000);
    timeHelper = Math.floor(timeHelper - (secs * 1000));
    var milis = timeHelper;

    hour = shiftZero(hour);
    minutes = shiftZero(minutes);
    secs = shiftZero(secs);
    milis = shiftMili(milis);

    return `${dateSql} ${hour}:${minutes}:${secs}.${milis}`;
  }

  function fetchData(buf) {
    var rows = [];

    const queryTypeNamedLenght = this.columnsType.queryTypeNamed.length;
    const numberOfRows = this.colSzs.rows;
    const colSzs = this.colSzs;
    var currentField;
    var firstBufferSize = 0;
    var currentOffSet = 10;
    var value;

    //prapare rowData ahead
    for (var readBufferRows=0; readBufferRows < numberOfRows; readBufferRows++) {
      rows.push({});
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

    for(var colTypes=0; colTypes < queryTypeNamedLenght ; colTypes++) {
      this.fetchBufferByColType.currentVarcharPosition = 0;
      currentField = this.columnsType.queryTypeNamed[colTypes];

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

      for (var i=0; i < numberOfRows; i++) {
        if(currentField.nullable) {
          if (nullBuffer.slice(i,i+1)[0]) {
            value = null;
          } else {
            value = this.fetchBufferByColType[fieldType](dataBuffer,fieldSize, i, varcharBuffer);
          }
        } else {
          value = this.fetchBufferByColType[fieldType](dataBuffer,fieldSize, i, varcharBuffer);
        }
        rows[i][currentField.name] = value;
      }
    }

    return rows;
  }

  return {
    fetchBufferByColType: fetchBufferByColType,
    setColumnsType: setColumnsType,
    setColSzs: setColSzs,
    fetchData: fetchData,
    set_varchar_encoding: set_varchar_encoding 
  };
};
