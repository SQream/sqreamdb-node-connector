"use strict";
const iconv = require('iconv-lite');

const tenn = BigInt(10);

class SqNumeric {
  constructor(bigint, scale) {
    this.bigint = bigint | 0n;
    this.scale = scale | 0;
  }
  toJSON() {
    let num = this.bigint.toString();
    const neg = this.bigint < 0n;
    const numLen = num.length - (neg ? 1 : 0);
    const start = this.scale > 0 ? [(neg ? '-' : ''), (this.scale >= numLen ? '0' : '')] : [];
    const decimalZeros = this.bigint && numLen < this.scale  ? Array(this.scale - numLen).fill('0') : [];
    return this.scale > 0 ? [...start, num.slice(neg ? 1 : 0, -1 * this.scale),  ".", ...decimalZeros,  num.slice(-1 * this.scale).replace(/0+$/, '') || '0'].join('') : num;;
  }
  toString() { 
    return this.toJSON();
  }

  static from(value, scale) {
    if (value === undefined) {
      value = BigInt(0);
    }
    if (value instanceof SqNumeric) {
      const diff = value.scale - (scale || 0) | 0;
      if (diff >= 0) {
        return new SqNumeric(value.bigint / (tenn ** BigInt(diff)), scale);
      }
      return new SqNumeric(value.bigint * (tenn ** BigInt(-diff)), scale);
    }
    if (typeof value === 'bigint') {
      scale = (scale || 0) | 0;
      return new SqNumeric(value * (tenn ** BigInt(scale)), scale);
    }
    value = value.toString();
    const split = (value + "").split(".");
    let extra = 0;
    let trim = 0;
    const l = (split[1] || "").length;
    if (scale !== undefined) {
      scale = scale | 0;
      const diff = l - scale;
      extra = diff > 0 ? 0 : -diff;
      trim = diff < 0 ? 0 : diff
    } else {
      scale = l;
    }
    if (scale < 0) throw new Error("Invalid scale: " + scale);

    return new SqNumeric(BigInt(split.concat(new Array(extra).fill('0')).join("").slice(0, trim > 0 ? -trim : undefined)), scale);
  }
}

module.exports = {
  SqNumeric: SqNumeric,
  ParseSqreamData : function ParseSqreamData() {
    let varchar_encoding = 'ascii';
    function setVarcharEncoding(encoding) {
      varchar_encoding = encoding || 'ascii';
    }

    const p8 = 2 ** 8;
    const p16 = 2 ** 16;
    const p24 = 2 ** 24;

    function readBigIntLE(buffer, offset = 0, bytesLength = 16) {
      if (buffer[offset + bytesLength - 1] === undefined)
        throw new Error('Out of bounds');
      let val = 0n;
      let last = true;
      for (let i = bytesLength - 4; i >= 0; i -= 4) {
        val = val << 32n;
        let value = 0;
        if (last) {
          value += buffer[offset + i + 3] << 24; // Overflow
          last = false;
        } else {
          value += buffer[offset + i + 3] * p24;
        }
        value += buffer[offset + i + 2] * p16;
        value += buffer[offset + i + 1] * p8;
        value += buffer[offset + i];

        val += BigInt(value);
      }
      return val;
    }

    let currentVarcharPosition = 0;

    const fetchBufferByColType = {
      ftNumeric: function (dataBuffer, fieldSize, i, vc, scale) {
        return new SqNumeric(readBigIntLE(dataBuffer, i * fieldSize, 16), scale);
      },
      ftLong: function (dataBuffer, fieldSize, i) {
        return dataBuffer.readBigInt64LE(i * fieldSize);
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

      const numericScale = {};
      columnsType.queryTypeNamed.forEach((col) => {
        if (col.type[0] === 'ftNumeric') numericScale[col.name] = col.type[2];
        if (!fetchBufferByColType[col.type[0]]) throw new Error("Unrecognized field type by connector: " + col.type[0]);
      });

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
        
        const fieldCb = fetchBufferByColType[fieldType];
        const scale = numericScale[currentField.name];

        for (let i = 0; i < numberOfRows; i++) {
          if(currentField.nullable) {
            if (nullBuffer[i]) {
              value = null;
            } else {
              value = fieldCb(dataBuffer,fieldSize, i, varcharBuffer, scale);
            }
          } else {
            value = fieldCb(dataBuffer,fieldSize, i, varcharBuffer, scale);
          }
          rows[i][currentField.name] = value;
        }
      }

      return rows;
    }

    return {
      fetchBufferByColType: fetchBufferByColType,
      fetchData: fetchData,
      setVarcharEncoding: setVarcharEncoding,
    };
  }
};
