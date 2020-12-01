const expect = require('chai').expect;
const Connection = require('../index');
const { step } = require('mocha-steps');

const config = {
  host: process.env.SQHOST || '127.0.0.1',
  port: process.env.SQPORT || 3108,
  username: 'sqream',
  password: 'sqream',
  connectDatabase: 'master',
  cluster: true,
  is_ssl: false
};


const types = {
  'BOOL': 1,
  'TINYINT': 1,
  'SMALLINT': 2,
  'INT': 4,
  'BIGINT': 8,
  'REAL': 4,
  'DOUBLE': 8,
  'TEXT': 6,
  'VARCHAR(10)': 10,
  'DATE': 4,
  'DATETIME': 8,
  // 'NUMERIC(38, 10)': 16 // uncomment when server is fixed
};

const samples = {
  'BOOL': 1,
  'TINYINT': 1,
  'SMALLINT': 2,
  'INT': 1,
  'BIGINT': BigInt(1),
  'REAL': 1,
  'DOUBLE': 1,
  'TEXT': 'abcdef',
  'VARCHAR(10)': 'abcdef',
  'DATE': '2020-02-02',
  'DATETIME': '2020-02-02 20:20:22',
  'NUMERIC(38, 10)': 12.34
}

const conn = new Connection(config);


const columnCount = 10;
let rowCount = 1000000;

describe(`Insert ${rowCount.toLocaleString()} rows`, function() {

  step(`Create table`, async function() {
    let cols = Object.keys(types).map((type) => [...Array(columnCount)].map((x, i) => `"${type}_${i}" ${type}`).join(", ")).join(", ");
    await conn.execute(`create or replace table insert_perf (${cols})`);
  });

  for (let type in types) {
    step(`Insert ${columnCount} ${type}`, async () => {
      const cols = [...Array(columnCount)].map((x, i) => `"${type}_${i}"`).join(", ")
      const putter = await conn.executeInsert(`insert into insert_perf (${cols}) values (${[...Array(columnCount)].map((x, i) => `?`).join(", ")})`);
      const start = Date.now();
      try {
        const row = [...Array(columnCount)].map((x, i) => samples[type]);
        for (let i = 0; i < rowCount; i++) {
          await putter.putRow(row);
        }
        await putter.flush();
        await putter.close();
        const per1MBytes = Math.floor(putter.getByteCount() / ((Date.now() - start) / 1000));
        console.log(`${type} time: `, Date.now() - start, 'ms', ', bytes per second:', per1MBytes.toLocaleString());
      } catch (e) {
        await putter.close();
        throw e;
      }
    });
  }
  
  step(`Create table not null`, async function() {
    await new Promise((res) => setTimeout(res, 10000));
    let cols = Object.keys(types).map((type) => [...Array(columnCount)].map((x, i) => `"${type}_${i}" ${type} not null`).join(", ")).join(", ");
    await conn.execute(`create or replace table insert_perf (${cols})`);
  });

  step(`Insert ALL (${columnCount * Object.keys(types).length} columns)`, async function() {
    let row = Object.keys(types).map((type) => [...Array(columnCount)].map((x, i) => samples[type])).flat();
    const putter = await conn.executeInsert(`insert into insert_perf values (${row.map(() => "?").join(", ")})`);
    let byteCount = 0;
    const start = Date.now();
    try {
      for (let i = 0; i < rowCount; i++) {
        await putter.putRow(row);
        if (putter.getByteCount() != byteCount) {
          byteCount = putter.getByteCount();
        }
      }
      await putter.flush();
      const per1MBytes = Math.floor(putter.getByteCount() / ((Date.now() - start) / 1000));
      console.log('Total bytes', putter.getByteCount().toLocaleString(), `, Time: `, Date.now() - start, 'ms', ', bytes per second:', per1MBytes.toLocaleString());
      await putter.close();
    } catch (e) {
      await putter.close();
      throw e;
    }
  });

  // Uncomment when server fixed
  // step(`Insert ${(rowCount * 3).toLocaleString()} ALL - concurrent`, async () => {
  //   let cols = Object.keys(types).map((type) => [...Array(columnCount)].map((x, i) => `"${type}_${i}" ${type} not null`).join(", ")).join(", ");
  //   await conn.execute(`create or replace table insert_perf (${cols})`);
    
  //   const promises = [];
  //   rowCount *= 3;
  //   let row = Object.keys(types).map((type) => [...Array(columnCount)].map((x, i) => samples[type])).flat();
  //   const start = Date.now();
  //   let totalBytes = 0;
  //   for (let k = 0; k < 3; k += 1) {
  //     promises.push((async () => {
  //       const putter = await conn.executeInsert(`insert into insert_perf values (${row.map(() => "?").join(", ")})`);
  //       for (let i = 0; i < rowCount; i++) {
  //         putter.putRow(row);
  //       }
  //       await putter.flush();
  //       totalBytes += putter.getByteCount();
  //       await putter.close();
  //     })());
  //   }
  //   await Promise.all(promises);
  //   const per1MBytes = Math.floor(totalBytes / ((Date.now() - start) / 1000));
  //   console.log('Total bytes', totalBytes, `, Time: `, Date.now() - start, 'ms', ', bytes per second:', per1MBytes.toLocaleString());
  // });

});