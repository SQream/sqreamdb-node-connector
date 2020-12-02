const iconv = require('iconv-lite');
const { SqNumeric } = require('./SqreamDataParser');
const util = require('util');
const TextEncoder = util.TextEncoder;

/**
 * 
 * @param {number} year 
 * @param {number} month 
 * @param {number} day 
 * @param {number} minutes 
 * @param {number} seconds 
 * @param {number} millis 
 */
function convertDate(year, month, day, hours, minutes, seconds, millis) {
  month = (month + 9) % 12;
  year = year - Math.floor(month / 10);

  const date = (365 * year + Math.floor(year / 4) - Math.floor(year / 100) + Math.floor(year / 400) + Math.floor((month * 306 + 5) / 10) + (day - 1));
  if (hours === undefined) return [date];

  const time = (hours | 0) * 3600000 + (minutes | 0) * 60000 + (seconds || 0) * 1000 + (millis | 0);
  return [date, time];
}

const space = " ".charCodeAt(0);
const nbits64 = BigInt(2) ** BigInt(64) - BigInt(1);
const n64 = BigInt(64);
const defaultMaxBytes = 200 * (2 ** 20);
const encoder = new TextEncoder();

const bitconverter = {
  ftBool: (buf, offset, value) => {
    buf(1).writeUInt8(value & 1, offset);
    return 1;
  },
  ftUByte: (buf, offset, value) => {
    buf(1).writeUInt8(value & 0xff, offset);
    return 1;
  },
  ftShort: (buf, offset, value) => {
    buf(2).writeInt16LE(value & 0xffff, offset);
    return 2;
  },
  ftInt: (buf, offset, value) => {
    buf(4).writeInt32LE(value | 0, offset);
    return 4;
  },
  ftFloat: (buf, offset, value) => {
    buf(4).writeFloatLE(+value, offset);
    return 4;
  },
  ftLong: (buf, offset, value) => {
    buf(8).writeBigInt64LE(BigInt(value), offset);
    return 8;
  },
  ftDouble: (buf, offset, value) => {
    buf(8).writeDoubleLE(+value, offset);
    return 8;
  },
  ftBlob: (buf, offset, value, fieldSize) => {
    if (value instanceof Buffer) {
    } else if (typeof value === 'string') {
      value = Buffer.from(encoder.encode(value));
    } else {
      throw new Error("Invalid value for ftBlob")
    }
    value.copy(buf(value.length), offset);
    return value.length;
  },
  ftDate: (buf, offset, value) => {
    const date = new Date(value);
    buf(4).writeInt32LE(convertDate(date.getFullYear(), date.getMonth() + 1, date.getDate())[0], offset);
    return 4;
  },
  ftDateTime: (buf, offset, value) => {
    let date = new Date(value);
    if (typeof value === "string" && (value.match(/Z$/) || !value.includes(":"))) {
      date = new Date(date.getTime() + new Date().getTimezoneOffset() * 60 * 1000);
    }
    const converted = convertDate(date.getFullYear(), 
                                  date.getMonth() + 1, 
                                  date.getDate(), 
                                  date.getHours(), 
                                  date.getMinutes(), 
                                  date.getSeconds(), 
                                  date.getMilliseconds());
    buf(4).writeInt32LE(converted[1], offset);
    buf(4).writeInt32LE(converted[0], offset + 4);
    return 8;
  },
  ftVarchar: (buf, offset, value, fieldSize, encoding) => {
    if (typeof value !== 'string') throw new Error("Invalid varchar offset, value: " + value);
    value = value + "";
    const spaces = value.length > fieldSize ? 0 : fieldSize - value.length;
    const bytes = Buffer.from(iconv.encode(value.substring(0, fieldSize), encoding));
    bytes.copy(buf(bytes.length), offset);
    Buffer.from(new Uint8Array(spaces).fill(space)).copy(buf(spaces), offset + bytes.length);
    return fieldSize;
  },
  ftNumeric: (buf, offset, value, fieldSize, encoding, scale) => {
    scale = scale | 0;
    if (scale < 0) throw new Error("Invalid scale");
    let num = SqNumeric.from(value, scale).bigint;
    buf(8).writeBigUInt64LE(num & nbits64, offset);
    buf(8).writeBigInt64LE(num >> n64, offset + 8);
    return 16;
  }
}

module.exports = {
  SqNumeric: SqNumeric,
  createInsertBuffer: function (queryTypeIn, varcharEncoding, maxBytes) {
    maxBytes = maxBytes || defaultMaxBytes;
    const perRow = queryTypeIn.reduce((sum, meta) => (meta.type[1] ? meta.type[1] : 104) + sum + meta.nullable, 0);
    const colSizes = queryTypeIn.map((meta) => Math.ceil(((meta.type[1] ? meta.type[1] : 104) + meta.nullable) / perRow * maxBytes));
    let byteArrayCols = queryTypeIn.map((m, i) => Buffer.alloc(colSizes[i]));
    let nulls = queryTypeIn.map(() => []);
    let nvarcharlengths = queryTypeIn.map(() => []);
    let colPositions = queryTypeIn.map(() => 0);
    let rowCount = 0;
    let byteCount = 0;
    const views = queryTypeIn.map((meta, columnIndex) => {
      return (num) => {
        const d = byteArrayCols[columnIndex];
        colPositions[columnIndex] += num;
        if (colPositions[columnIndex] > d.byteLength) {
          const b = Buffer.alloc(d.byteLength * 2);
          d.copy(b);
          return byteArrayCols[columnIndex] = b;
        }
        return d;
      }
    })

    function getBuffer() {
      let buffArray = [];
      for (let idx = 0; idx < queryTypeIn.length; idx++) {
        const meta = queryTypeIn[idx];
        if (meta.nullable) {
          buffArray.push(Uint8Array.from(nulls[idx]));
        }
        if (meta.isTrueVarChar) {
          const nBufs = Buffer.allocUnsafe(nvarcharlengths[idx].length * 4);
          for (let i = 0; i < nvarcharlengths[idx].length; i++) {
            nBufs.writeUInt32LE(nvarcharlengths[idx][i], i * 4);
          }
          buffArray.push(nBufs);
        }
        buffArray.push(byteArrayCols[idx].slice(0, colPositions[idx]));
      }
      rowCount = 0;
      byteCount = 0;
      nulls = queryTypeIn.map(() => []);
      nvarcharlengths = queryTypeIn.map(() => []);
      colPositions = queryTypeIn.map(() => 0);
      const res = Buffer.concat(buffArray);
      byteArrayCols.forEach((buf, i) => buf.fill(0));
      return res;
    }

    function cancel() {
      rowCount = 0;
      byteCount = 0;
      nulls = queryTypeIn.map(() => []);
      byteArrayCols.forEach((buf, i) => buf.fill(0));
      nvarcharlengths = queryTypeIn.map(() => []);
      colPositions = queryTypeIn.map(() => 0);
    }

    function setRow(row) {
      if (row.length !== queryTypeIn.length) throw new Error("Unmatched number of columns in insert. Expected: " + queryTypeIn.length + ", Got: " + row.length);

      for (let idx = 0; idx < queryTypeIn.length; idx++) {
        const meta = queryTypeIn[idx];
        const value = row[idx];
        if (meta.nullable) {
          nulls[idx].push(value === null ? 1 : 0);
          byteCount++;
        }
        const type = meta.type[0];
        const size = meta.type[1];
        const scale = meta.type[2];
        if (!bitconverter[type]) {
          throw new Error("Unrecognized field type by connector: " + type);
        }
        let len = size;
        if (value === null) {
          views[idx](size);
        } else {
          len = bitconverter[type](views[idx], colPositions[idx], value, size, varcharEncoding, scale);
        }
        if (meta.isTrueVarChar) {
          nvarcharlengths[idx].push(len);
          byteCount += 4;
        }
        byteCount += len;
      }

      rowCount++;
      return byteCount < maxBytes;
    }

    function getRowCount() {
      return rowCount;
    }

    function getByteCount() {
      return byteCount;
    }

    return {
      setRow,
      getBuffer,
      getRowCount,
      cancel,
      getByteCount,
    }
  }
}