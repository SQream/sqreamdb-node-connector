const isDEV = false;

const fs = require('fs');
const async = require('async');
const path = require('path');
const Connection = require('../index');
const moment = require('moment');
const net = require('net');
const Stream = require('stream');
const readableStream =  require('stream').Readable;
const sqreamCppBuffer = require('../lib/addon/sqreamLongAddon.node');
const logFilePath =  __dirname+ '/erez.log';

var statementId;

if (fs.existsSync(logFilePath)) {
  fs.unlinkSync(logFilePath)
}

const logFile = fs.createWriteStream(logFilePath);
function writeToLogFile (msg) {
  if (isDEV) {
      console.log(msg);
      logFile.write(msg);
      logFile.write('\n');
  }
}
writeToLogFile('node.js start!');


function convertDateToInt(value) {
  var result = 0;
  if (value != null){
    const date = moment(value);
    var year = date.year();
    var month = date.month() + 1;
    var day = date.date();

    month = (month + 9) % 12;
    year  = year - parseInt(month/10);
    result = (365*year + parseInt(year/4) - parseInt(year/100) + parseInt(year/400) + parseInt((month*306 + 5)/10) + ( day - 1 ));
  }
  return result;
}
const dataTypeToBuffer = {
  getBuffer: function (value, columnType){
    if (this[columnType.type]) {
      return this[columnType.type](value, columnType)
    }
  },
  ftInt: function (value) {
    const buf = Buffer.alloc(4);
    buf.writeInt32LE(value);
    return buf;
  },
  ftLong: function (value) {
    const buf = Buffer.alloc(8);
    sqreamCppBuffer.longToBuffer(buf, value);
    return buf;
  },
  ftVarchar: function (value, columnType) {
    var buf = Buffer.alloc(columnType.len);
    var valToBuf;
    if (value) {
      valToBuf = value + ' '.repeat(columnType.len - value.length);
    } else {
        valToBuf = ' '.repeat(columnType.len);
    }

    buf.write(valToBuf, 0);
    return buf;
  },
  ftBlob: function (value, columnType) {
    var calcLen = columnType.len / 4;
    var buf = Buffer.alloc(calcLen);
    var valToBuf;
    if (value) {
        valToBuf = value + ' '.repeat(calcLen  - value.length);
    } else {
        valToBuf = ' '.repeat(calcLen);
    }
    buf.write(valToBuf, 0);
    return buf;
  },

  ftDouble: function (value) {
    var buf = Buffer.allocUnsafe(8);
    buf.writeDoubleLE(value, 0);

    return buf;
  },
  ftFloat: function (value) {
    var buf = Buffer.allocUnsafe(4);
    buf.writeFloatLE(value, 0);
    return buf;
  },
  ftBool: function (value) {
    var buf = Buffer.allocUnsafe(1);
    buf.writeUIntLE(value,0,1);
    return buf;
  },
  ftDate: function (value) {
    const buf = Buffer.allocUnsafe(4);
    const result = convertDateToInt(value);
    buf.writeInt32LE(parseInt(result),0);
    return buf;
  },
  ftDateTime: function (value) {
    var buf = Buffer.alloc(8);
    if (value != null){
      const dt = moment(value);
      var nTime = dt.hour() * 60 * 60;
      nTime = nTime + dt.minutes() * 60;
      nTime = nTime + dt.second();
      nTime = nTime + dt.format('SSS');
      var date = convertDateToInt(value);
      sqreamCppBuffer.dateTimeHelper(buf, date, nTime);
    } else {
      sqreamCppBuffer.dateTimeHelper(buf, 0, 0);
    }
    return buf;
  },

  ftUByte: function (value) {
    var buf = Buffer.allocUnsafe(1);
    buf.writeUInt8(value, 0);
    return buf;

  },
  ftShort: function (value) {
    var buf = Buffer.alloc(2);
    buf.writeInt16LE(value, 0, 2);
    return buf;
  }

};

class TestSqreamNodeConnector {
  constructor(config) {
    this.config = config;
  }

  outputTypes (types) {
    this.dataTypes = types;
    var type;
    var len;
    var output = [];
    var names = [];

    types.map( item => {
      type = item.type[0];
    len = item.type[1];

    if (item.name !== '?column?') {
        names.push(`[${item.name}]`);
    } else {
        names.push('[]');
    }

    switch(type){
      case 'ftVarchar':
        output.push(`varchar(${len})`);
        break;
      case 'ftBlob':
        output.push(`varchar(${(len/4)})`);
        break;
      case 'ftInt':
        if (len == 1){
          output.push('tinyint');
        } else  {
          output.push('int');
        }
        break;
      case 'ftLong':
        output.push('bigint');
        break;

      case 'ftBool':
        output.push('bit');
        break;

      case 'ftDouble':
        output.push('float');
        break;
      case 'ftFloat':
        output.push('real');
        break;
      case 'ftDate':
        output.push('date');
        break;
      case 'ftDateTime':
        output.push('datetime');
        break;
      case 'ftUByte':
        output.push('tinyint');
        break;

      case 'ftShort':
        output.push('smallint');
        break;
    }
  });

    const metaFile = path.join(this.config.dir,'meta.txt');
    const stream = fs.createWriteStream(metaFile);
    stream.once('open', function(fd) {
      stream.write(names.join(','));
      stream.write('\n');
      stream.write(output.join(','));
        stream.write('\n');
      //stream.write(output);
      stream.end();
    });
  }

  prepareTypeByName (types) {
    var typesByName = {};
    types.map( item => {
      typesByName[item.name] = {
        type: item.type[0],
        len: item.type[1],
        nullable: item.nullable,
        isTrueVarChar: item.isTrueVarChar
      };
  });

    return typesByName;
  }

  createDataFiles(types) {
    this.dataFiles = [];
    /// this.readStreams = [];
    var filePath;
    var writeStream;

    /*filePath = path.join(this.config.dir, '0.dat');
    this.testWriteStream = fs.createWriteStream(filePath);
    this.readStreams.push(new readableStream());
    this.readStreams[0].pipe(this.testWriteStream);*/

    // writeStream = fs.createWriteStream(filePath);
    for (var i=0; i< types.length*2; i++){
      filePath = path.join(this.config.dir, i + '.dat');
      writeStream = fs.createWriteStream(filePath);
      this.dataFiles.push(writeStream);
      // this.readStreams.push(new readableStream());
      // readStream = new readableStream();

      //this.readStreams[i].pipe(this.dataFiles[i]);
      // this.readStreams[i].pipe(process.stdout);

      //readStream.pipe(process.stdout);
      /*readStream.on('data', function(data){
        console.log('streamg ert data', data);
      })*/

      /*this.readStreams[i].on('data', (chunk) => {
        // console.log(arguments);
        console.log(`Received ${chunk.length} bytes of data.`);
      });*/
    }

  }

  outputData_liner (data) {
    this.createDataFiles(this.dataTypes);

    // this.dataFiles = [];
    // this.readStreams = [];
    // console.log('data.lenght=', data.length);

    //return;

    var typesByName = this.prepareTypeByName(this.dataTypes);
    var out;
    const self = this;

    data.map( (row, rowum) => {
      // console.log(rowum, row);
      Object.keys(row).forEach(function(key, index) {
        // console.log('index=',index);

        out = dataTypeToBuffer.getBuffer(row[key], typesByName[key]);
        const buf = Buffer.alloc(1);
        if (row[key] === null) {
          buf.writeIntLE(1,0,1);
        } else {
          // console.log(index);
          buf.writeIntLE(0,0,1);
        }
        const nullIndex = index*2+1;
        self.readStreams[nullIndex].push(buf);
         //self.dataFiles[nullIndex].write(buf);

        if (out){
          //self.dataFiles[index*2].write(out);

         self.readStreams[index*2].push(out);

        }
      });
  });

    self.readStreams.map( item => {
      item.push(null);
    });

    /*console.log('----------------');
    console.log(self.readStreams[0]);
    console.log('----------------');*/
    //console.log(self.readStreams[2]);
    var closeFilesFunctions = [];

    function closeSteam(stream,callback){
      return function(callback){
        stream.end(null, null, callback);
      }
    }

    self.dataFiles.map( item => {
      const cb = function () {
        return null;
      };
      closeFilesFunctions.push( closeSteam(item, cb))
    });

    async.series(
      closeFilesFunctions,
    // optional callback
      function(err, results) {
        var end= new Date;
        // console.log('end=', end);
        process.exit();
      });
  }

  outputData (data) {
    const self = this;
    const typesByName = this.prepareTypeByName(this.dataTypes);
    const totalLines = data.length;

    var out;

    this.createDataFiles(this.dataTypes);

    function writeToFile(writer, key,  callback) {
      var count = 0;
      write();
      function write() {
        var ok = true;
        do {
          //console.log(count, data[count]);
          out = dataTypeToBuffer.getBuffer(data[count][key], typesByName[key]);
          if (count === (totalLines - 1) ) {
            // last time!
            writer.write(out);
            callback();
          } else {
            // see if we should continue, or wait
            // don't pass the callback, because we're not done yet.
            ok = writer.write(out);
          }
          count++;
        } while (count < totalLines && ok);
        if (count < totalLines) {
          // had to stop early!
          // write some more once it drains
          writer.once('drain', write);
        }
      }
    }

    function writeNullToFile(writer, key,  callback) {
      var count = 0;
      write();
      function write() {
        var ok = true;
        do {
          const buf = Buffer.alloc(1);
          if (data[count][key] === null) {
            buf.writeIntLE(1,0,1);
          } else {
            // console.log(index);
            buf.writeIntLE(0,0,1);
          }

          if (count === (totalLines - 1) ) {
            // last time!
            writer.write(buf, null, callback);
          } else {
            // see if we should continue, or wait
            // don't pass the callback, because we're not done yet.
            ok = writer.write(buf);
          }
          count++;
        } while (count < totalLines && ok);
        if (count < totalLines) {
          // had to stop early!
          // write some more once it drains
          writer.once('drain', write);
        }
      }
    }

    var outputArray = [];
    function createData(writer, key ,callback){
      return function(callback){
        writeToFile(writer, key, callback);
      }
    }

    function createNullData(writer, key ,callback){
      return function(callback){
        writeNullToFile(writer, key, callback);
      }
    }

    Object.keys(typesByName).forEach(function(key, index) {
      const cb = function () {
        return null;
      };

      const nullIndex = index*2+1;
      outputArray.push( createData(self.dataFiles[index*2], key, cb));
      outputArray.push( createNullData(self.dataFiles[nullIndex], key, cb));
    });


    async.parallel(
      outputArray,
      // optional callback
      function(err, results) {
         // console.log('err', err, results)
        self.closeFiles();
      });
  }

  closeFiles() {
    const self = this;
    var functionsArray = [];

    function closeSteam(stream, callback){
      return function(callback){
        stream.end(null, null, callback);
      }
    }

    self.dataFiles.map( item => {
      const cb = function () {
        return null;
      };
      functionsArray.push( closeSteam(item, cb))
    });

    async.series(
      functionsArray,
      // optional callback
      function(err, results) {
        // console.log('err', err, results)
        // var end= new Date;
        //console.log('end=', end);
        process.exit();
      });
  }

  run_query () {
    writeToLogFile('start run_query()');
    const myConnection = new Connection(this.config);
    const self = this;

    myConnection.events.on('getTypes', function(data){
      self.outputTypes(data);
    });
/*
    myConnection.events.on('getStatemnetId', function(data){
      console.log('getStatemnetId', data);
      statementId = data;

      const stopConfig = {
        host: '127.0.0.1',
        port: 2687,
        username: 'sqream',
        password: 'sqream',
        connectDatabase: 'developer_regression_query'
      };

      const stopConnection = new Connection(stopConfig);
      const sqlStop = `SELECT stop_statement(${data.statement_id})`;
      console.log(sqlStop);
      stopConnection.runQuery(sqlStop, function (err, data) {
        console.log(err, data);

      });
    });
*/

    const cb = function (err, data) {
      if (err){
        writeToLogFile(JSON.stringify(err));
      }
      self.outputData(data);
    };
    myConnection.runQuery(this.config.sql, cb);
  }
}

var config;
if (isDEV) {
  // config for dev local run
  config = {
    dir: '/tmp/test_connector',
    host: '127.0.0.1',
    port: 2687,
    username: 'sqream',
    password: 'sqream',
    //connectDatabase: 'hourly_quick',
    connectDatabase: 'developer_regression_query',
    //connectDatabase: 'only_datetime',
    //connectDatabase: 'utf8',
    // sql: "SELECT 9223372036854775807;"
    // sql: "select top 10 t_d.xfloat,t_e.xfloat from t_d inner join t_e on t_d.xfloat_2 - t_d.xfloat <= t_e.xfloat where t_e.xint < 100 and t_d.xint <100"
    // sql: "select top 5000 t_d.xfloat,t_e.xfloat from t_d inner join t_e on t_d.xfloat_2 - t_d.xfloat <= t_e.xfloat where t_e.xint < 100 and t_d.xint <100"
    //sql: "select t_a.xint , max(t_a.xint) over (partition by t_b.xint) from t_a right join t_b on t_a.xint = t_b.xint where t_a.xint < 3"
    // sql: "SELECT DATEADD(year,16,xdatetime) AS newDate from t_a"
    //sql: "select t_a.xdatetime from t_a,t_b,t_c where t_a.xdatetime >= t_b.xdatetime and t_a.xdatetime >= t_c.xdatetime and t_a.xint < 1000 and t_b.xint < 1000 and t_c.xint < 1000"
    // sql: "select top 10 t_d.xfloat from t_d inner join t_e on t_d.xfloat_2 - t_d.xfloat <= t_e.xfloat where t_e.xint < 100 and t_d.xint <100"
     // sql: "select sum(xreal)* 0.5 \/ count(xreal) + max(xreal)-min(xreal)+ 0 + count(xreal)\/avg(xreal)+1 from t_a group by xint_2"
     //sql: "select sum(xreal)* 0.5 / count(xreal) + max(xreal)-min(xreal)+ 0 + count(xreal)/avg(xreal)+1 from t_a group by xint_2"
     sql: "select xdatetime from t_a order by xdatetime"

  };

} else {
  config = {
    host: '127.0.0.1',
    username: 'sqream',
    password: 'sqream',
    connectDatabase: 'master'
  };

  if (process.argv.length < 6){
    console.log('arguments are: <dir> <sql statement> <database> <port> <sqream ip> <cluster true/false> <user> <password>\n');
    return;
  }

  config.dir = process.argv[2];
  //config.sql = process.argv[3].replace('\\/','/');
  //config.sql = process.argv[3].replace('\\/','/').replace('\\\/','/');
  const sql_regex = /\\\//g;
  //console.log(process.argv[3].replace(regex ,'/'));
  config.sql = process.argv[3].replace(sql_regex ,'/');
  config.connectDatabase = process.argv[4];
  config.port = process.argv[5];


  if (process.argv.length >= 6) {
    config.host = process.argv[6];
  }

  if (process.argv.length >= 7) {
    config.cluster = process.argv[7];
  }
  if (process.argv.length >= 8) {
    config.user = process.argv[8];
  }

  if (process.argv.length >= 9) {
    config.password = process.argv[9];
  }
}
/*
console.log(process.argv[3]);
console.log("sql");
console.log(config.sql);
*/

var start = new Date;
// console.log('start=',start);

const testSqreamNodeConnector = new TestSqreamNodeConnector(config);
testSqreamNodeConnector.run_query();


/*
setTimeout(function () {
  console.log('statementId=', statementId)
}, 100);*/
