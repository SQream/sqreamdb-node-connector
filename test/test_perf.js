const expect = require('chai').expect;
const Connection = require('../index');
const { step } = require('mocha-steps');

function columnName(type, props = {}) {
  const col = new URL(type, "ws://a");
  for (let p in props) {
    col.searchParams.append(p, props[p].toString())
  }
  return col.toString().substring(7);
}

const config = {
  host: process.env.SQHOST || '127.0.0.1',
  port: process.env.SQPORT || 6000,
  username: 'sqream',
  password: 'sqream',
  connectDatabase: 'master',
  cluster: false,
  is_ssl: false
};

const conn = new Connection(config);

const types = {
  'BOOL': 1,
  'TINYINT': 1,
  'SMALLINT': 2,
  'INT': 4,
  'BIGINT': 8,
  'REAL': 4,
  'DOUBLE': 8,
  'TEXT': 10,
  'VARCHAR': 10,
  'DATE': 4,
  'DATETIME': 8, 
};
const columnCounts = [1, 10, 100];
const varcharSizes = [10, 100, 400];
const rowCounts = [1, 1000, 10000, 100000, 1000000];

const logs = [];

describe("Warmup", () => {
  step("all columns", async () => {
    const columnCount = (200 / Object.keys(types).length)|0;
    const rowCount = 100000;
    let cols = Object.keys(types).map((type) => [...Array(columnCount)].map((x, i) => `"${columnName(type, {name: i})}"`).join(", ")).join(", ");
    const it = await conn.executeCursor(`select ${cols} from random limit ${rowCount}`);
    const start = Date.now();
    try {
      for await (let rows of it.fetchIterator()) {
      }
    } catch (err) {
    }
    await it.close();
  });
})

for (let type in types) {
  describe(type, function() {
    for (let columnCount of columnCounts) {
      for (let rowCount of rowCounts) {
        if (['TEXT', 'VARCHAR'].includes(type)) {
          for (let vSize of varcharSizes) {
            step(`Columns ${columnCount}, column size ${vSize}, row count ${rowCount}`, async function() {
              let cols = [...Array(columnCount)].map((x, i) => `"${columnName(type, {name: i, length: vSize})}"`).join(", ");
              let count = 0;
              const it = await conn.executeCursor(`select ${cols} from random limit ${rowCount}`);
              const start = Date.now();
              try {
                for await (let rows of it.fetchIterator()) {
                  count += rows.length;
                }
              } catch (err) {
                await it.close();
                throw err;
              }
              const totalTime = Date.now() - start;
              await it.close();
              expect(count).to.eq(rowCount);
              const rowlength = vSize * columnCount;
              const timePerMillion = (totalTime / (rowCount * rowlength) * 1000000)|0;
              logs.push({field: type, "row length": rowlength, columns: columnCount, rows: rowCount, "total ms": totalTime, "per 1M bytes": timePerMillion});
            });
          }
        } else {
          step(`Columns ${columnCount}, column size ${types[type]}, row count ${rowCount}`, async function() {
            let cols = [...Array(columnCount)].map((x, i) => `"${columnName(type, {name: i})}"`).join(", ");
            let count = 0;
            const it = await conn.executeCursor(`select ${cols} from random limit ${rowCount}`);
            const start = Date.now();
            try {
              for await (let rows of it.fetchIterator()) {
                count += rows.length;
              }
            } catch (err) {
              await it.close();
              throw err;
            }
            const totalTime = Date.now() - start;
            await it.close();
            expect(count).to.eq(rowCount);
            const rowlength = types[type] * columnCount;
            const timePerMillion = (totalTime / (rowCount * rowlength) * 1000000)|0;
            logs.push({field: type, "row length": rowlength, columns: columnCount, rows: rowCount, "total ms": totalTime, "per 1M bytes": timePerMillion});
          });
        }
      }
    }
  });
}

describe('All types', function() {
  step(`200 Columns`, async function() {
    let count = 0;
    const columnCount = (200 / Object.keys(types).length)|0;
    const rowCount = 100000;
    let cols = Object.keys(types).map((type) => [...Array(columnCount)].map((x, i) => `"${columnName(type, {name: i})}"`).join(", ")).join(", ");
    const it = await conn.executeCursor(`select ${cols} from random limit ${rowCount}`);
    const start = Date.now();
    try {
      for await (let rows of it.fetchIterator()) {
        count += rows.length;
      }
    } catch (err) {
      await it.close();
      throw err;
    }
    const totalTime = Date.now() - start;
    await it.close();
    expect(count).to.eq(rowCount);
    const rowlength = Object.values(types).map((l) => l * columnCount).reduce((sum, n) => sum + n, 0);
    const timePerMillion = (totalTime / (rowCount * rowlength) * 1000000)|0;
    logs.push({field: "ALL", "row length": rowlength, columns: columnCount * Object.keys(types).length, rows: rowCount, "total ms": totalTime, "per 1M bytes": timePerMillion});
  });
});

describe('Results', function() {
  step(`Results`, async function() {
    console.table(logs)
  });
});
