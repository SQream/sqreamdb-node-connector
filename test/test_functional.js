const should = require('chai').should();
const expect = require('chai').expect;
const Connection = require('../index');
const { step } = require('mocha-steps');

const config = {
    host: process.env.SQHOST || '127.0.0.1',
    port: process.env.SQPORT || 5000,
    username: 'sqream',
    password: 'sqream',
    connectDatabase: 'master',
    cluster: false,
    is_ssl: false
};


function runQueryPromise(query) {
    return new Promise(function (resolve) {
        const myConnection = new Connection(config);
        myConnection.runQuery(query, function (err, data){
            resolve({ err: err, data: data });
        }, null );
    })
}

function runInsert(query) {
    const myConnection = new Connection(config);
    return myConnection.executeInsert(query);    
}

describe('Boolean Table', function() {
    step('Create Bool Table', async function() {
        const res = await runQueryPromise('CREATE OR REPLACE TABLE test (bool_column bool)');
        should.not.exist(res.err);
    });

    step('Insert into Bool Table', async function() {
        // const res = await runQueryPromise('INSERT INTO test VALUES (false)');
        // should.not.exist(res.err);
        const putter = await runInsert('INSERT INTO test VALUES (?)');
        putter.putRow([false]);
        await putter.flush();
        await putter.close();
    });

    step('Fetch Bool value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        expect(val).to.equal(0);
    });

    step('Insert invalid Bool value', async function() {
        const res = await runQueryPromise("insert into test values ('foo')");
        should.exist(res.err);
    });

    step('Clean up Boolean table', async function() {
        const res = await runQueryPromise('DROP TABLE test');
        should.not.exist(res.err);
    });
});

describe('TINYINT Table', function() {
    step('Create TINYINT Table', async function() {
        const res = await runQueryPromise('CREATE OR REPLACE TABLE test (tinyint_column TINYINT)');
        should.not.exist(res.err);
    });

    step('Insert into TINYINT Table', async function() {
        // const res = await runQueryPromise('INSERT INTO test VALUES (1)');
        // should.not.exist(res.err);
        const putter = await runInsert('INSERT INTO test VALUES (?)');
        putter.putRow([1]);
        await putter.flush();
        await putter.close();
    });

    step('Fetch TINYINT value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        expect(val).to.equal(1);
    });

    step('Insert invalid TINYINT value', async function() {
        const res = await runQueryPromise('insert into test values (40000)');
        should.exist(res.err);
    });

    step('Clean up TINYINT table', async function() {
        const res = await runQueryPromise('DROP TABLE test');
        should.not.exist(res.err);
    });
});


describe('SMALLINT Table', function() {
    step('Create SMALLINT Table', async function() {
        const res = await runQueryPromise('CREATE OR REPLACE TABLE test (sint_column SMALLINT)');
        should.not.exist(res.err);
    });

    step('Insert into SMALLINT Table', async function() {
        // const res = await runQueryPromise('INSERT INTO test VALUES (1)');
        // should.not.exist(res.err);
        const putter = await runInsert('INSERT INTO test VALUES (?)');
        putter.putRow([1]);
        await putter.flush();
        await putter.close();
    });

    step('Fetch SMALLINT value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        expect(val).to.equal(1);
    });

    step('Insert invalid SMALLINT value', async function() {
        const res = await runQueryPromise('insert into test values (40000)');
        should.exist(res.err);
    });

    step('Clean up SMALLINT table', async function() {
        const res = await runQueryPromise('DROP TABLE test');
        should.not.exist(res.err);
    });
});

describe('INT Table', function() {
    step('Create INT Table', async function() {
        const res = await runQueryPromise('CREATE OR REPLACE TABLE test (sint_column INT)');
        should.not.exist(res.err);
    });

    step('Insert into INT Table', async function() {
        // const res = await runQueryPromise('INSERT INTO test VALUES (1)');
        // should.not.exist(res.err);
        const putter = await runInsert('INSERT INTO test VALUES (?)');
        putter.putRow([1]);
        await putter.flush();
        await putter.close();
    });

    step('Fetch INT value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        expect(val).to.equal(1);
    });

    step('Insert invalid INT value', async function() {
        const res = await runQueryPromise('insert into test values (3000000000)');
        should.exist(res.err);
    });

    step('Clean up INT table', async function() {
        const res = await runQueryPromise('DROP TABLE test');
        should.not.exist(res.err);
    });
});



describe('BIGINT Table', function() {
    step('Create BIGINT Table', async function() {
        const res = await runQueryPromise('CREATE OR REPLACE TABLE test (bigint_column BIGINT)');
        should.not.exist(res.err);
    });

    step('Insert into BIGINT Table', async function() {
        // const res = await runQueryPromise('INSERT INTO test VALUES (1)');
        // should.not.exist(res.err);
        const putter = await runInsert('INSERT INTO test VALUES (?)');
        putter.putRow([1]);
        await putter.flush();
        await putter.close();
    });

    step('Fetch BIGINT value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        expect(val).to.equal(1n);
    });

    step('Insert invalid BIGINT value', async function() {
        const res = await runQueryPromise('INSERT INTO test values ("false")');
        should.exist(res.err);
    });

    step('Clean up BIGINT table', async function() {
        const res = await runQueryPromise('DROP TABLE test');
        should.not.exist(res.err);
    });
});

describe('FLOAT Table', function() {
    step('Create FLOAT Table', async function() {
        const res = await runQueryPromise('CREATE OR REPLACE TABLE test (bigint_column FLOAT)');
        should.not.exist(res.err);
    });

    step('Insert into FLOAT Table', async function() {
        // const res = await runQueryPromise('INSERT INTO test VALUES (1.0)');
        // should.not.exist(res.err);
        const putter = await runInsert('INSERT INTO test VALUES (?)');
        putter.putRow([1.0]);
        await putter.flush();
        await putter.close();
    });

    step('Fetch FLOAT value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        expect(val).to.equal(1.0);
    });

    step('Insert invalid FLOAT value', async function() {
        const res = await runQueryPromise('INSERT INTO test values ("false")');
        should.exist(res.err);
    });

    step('Clean up FLOAT table', async function() {
        const res = await runQueryPromise('DROP TABLE test');
        should.not.exist(res.err);
    });
});

describe('REAL Table', function() {
    step('Create REAL Table', async function() {
        const res = await runQueryPromise('CREATE OR REPLACE TABLE test (bigint_column REAL)');
        should.not.exist(res.err);
    });

    step('Insert into REAL Table', async function() {
        // const res = await runQueryPromise('INSERT INTO test VALUES (1.0)');
        // should.not.exist(res.err);
        const putter = await runInsert('INSERT INTO test VALUES (?)');
        putter.putRow([1.0]);
        await putter.flush();
        await putter.close();
    });

    step('Fetch REAL value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        expect(val).to.equal(1.0);
    });

    step('Insert invalid REAL value', async function() {
        const res = await runQueryPromise('INSERT INTO test values ("false")');
        should.exist(res.err);
    });

    step('Clean up REAL table', async function() {
        const res = await runQueryPromise('DROP TABLE test');
        should.not.exist(res.err);
    });
});

describe('DATE Table', function() {
    step('Create DATE Table', async function() {
        const res = await runQueryPromise('CREATE OR REPLACE TABLE test (bigint_column DATE)');
        should.not.exist(res.err);
    });

    step('Insert into DATE Table', async function() {
        // const res = await runQueryPromise("INSERT INTO test VALUES ('2010-10-10')");
        // should.not.exist(res.err);
        const putter = await runInsert('INSERT INTO test VALUES (?)');
        putter.putRow(['2010-10-10']);
        await putter.flush();
        await putter.close();
    });

    step('Fetch DATE value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        expect(val).to.equal("2010-10-10");
    });

    step('Insert invalid DATE value', async function() {
        const res = await runQueryPromise('INSERT INTO test values ("false")');
        should.exist(res.err);
    });

    step('Clean up DATE table', async function() {
        const res = await runQueryPromise('DROP TABLE test');
        should.not.exist(res.err);
    });
});

describe('DATETIME Table', function() {
    step('Create DATETIME Table', async function() {
        const res = await runQueryPromise('CREATE OR REPLACE TABLE test (bigint_column DATETIME)');
        should.not.exist(res.err);
    });

    step('Insert into DATETIME Table', async function() {
        // const res = await runQueryPromise("INSERT INTO test VALUES ('2010-10-10 23:59:59')");
        // should.not.exist(res.err);
        const putter = await runInsert('INSERT INTO test VALUES (?)');
        putter.putRow(['2010-10-10 23:59:59']);
        await putter.flush();
        await putter.close();
    });

    step('Fetch DATETIME value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        expect(val).to.equal("2010-10-10 23:59:59.000");
    });

    step('Insert invalid DATETIME value', async function() {
        const res = await runQueryPromise('INSERT INTO test values ("false")');
        should.exist(res.err);
    });

    step('Clean up DATETIME table', async function() {
        const res = await runQueryPromise('DROP TABLE test');
        should.not.exist(res.err);
    });
});
 

describe('TIMESTAMP Table', function() {
    step('Create TIMESTAMP Table', async function() {
        const res = await runQueryPromise('CREATE OR REPLACE TABLE test (bigint_column TIMESTAMP)');
        should.not.exist(res.err);
    });

    step('Insert into TIMESTAMP Table', async function() {
        // const res = await runQueryPromise("INSERT INTO test VALUES ('2010-10-10')");
        // should.not.exist(res.err);
        const putter = await runInsert('INSERT INTO test VALUES (?)');
        putter.putRow(['2010-10-10']);
        await putter.flush();
        await putter.close();
    });

    step('Fetch TIMESTAMP value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        expect(val).to.equal("2010-10-10 00:00:00.000");
    });

    step('Insert invalid TIMESTAMP value', async function() {
        const res = await runQueryPromise('INSERT INTO test values ("false")');
        should.exist(res.err);
    });

    step('Clean up TIMESTAMP table', async function() {
        const res = await runQueryPromise('DROP TABLE test');
        should.not.exist(res.err);
    });
});

describe('VARCHAR Table', function() {
    step('Create VARCHAR Table', async function() {
        const res = await runQueryPromise('CREATE OR REPLACE TABLE test (t VARCHAR(20))');
        should.not.exist(res.err);
    });

    step('Insert into VARCHAR Table', async function() {
        // const res = await runQueryPromise("INSERT INTO test VALUES ('test')");
        // should.not.exist(res.err);
        const putter = await runInsert('INSERT INTO test VALUES (?)');
        putter.putRow(['test']);
        await putter.flush();
        await putter.close();
    });

    step('Fetch VARCHAR value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        expect(val.trim()).to.equal("test");
    });

    step('Insert invalid VARCHAR value', async function() {
        const res = await runQueryPromise('INSERT INTO test values ("this is more than just 20 characters, no need to count")');
        should.exist(res.err);
    });

    step('Clean up VARCHAR table', async function() {
        const res = await runQueryPromise('DROP TABLE test');
        should.not.exist(res.err);
    });
});

describe('NVARCHAR Table', function() {
    step('Create NVARCHAR Table', async function() {
        const res = await runQueryPromise('CREATE OR REPLACE TABLE test (t NVARCHAR(20))');
        should.not.exist(res.err);
    });

    step('Insert into NVARCHAR Table', async function() {
        // const res = await runQueryPromise("INSERT INTO test VALUES ('test'), (NULL)");
        // should.not.exist(res.err);
        const putter = await runInsert('INSERT INTO test VALUES (?)');
        putter.putRow(['test']);
        putter.putRow([null]);
        await putter.flush();
        await putter.close();
    });

    step('Fetch NVARCHAR value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        const nul = res.data[1][keys[0]];
        expect(val.trim()).to.equal("test");
        expect(nul).to.equal(null);
    });

    step('Insert invalid NVARCHAR value', async function() {
        const res = await runQueryPromise('INSERT INTO test values ("this is more than just 20 characters, no need to count")');
        should.exist(res.err);
    });

    step('Clean up NVARCHAR table', async function() {
        const res = await runQueryPromise('DROP TABLE test');
        should.not.exist(res.err);
    });
});


describe('NUMERIC Table', function() {
    step('Create NUMERIC Table', async function() {
        const res = await runQueryPromise('CREATE OR REPLACE TABLE test (numeric_column NUMERIC(4, 2))');
        should.not.exist(res.err);
    });

    step('Insert into NUMERIC Table', async function() {
        // const res = await runQueryPromise('INSERT INTO test VALUES (12.34), (1), (-1), (0), (0.1), (0.01), (10.0)');
        // should.not.exist(res.err);
        const putter = await runInsert('INSERT INTO test VALUES (?)');
        putter.putRow([12.34]);
        putter.putRow([1]);
        putter.putRow([-1]);
        putter.putRow([0]);
        putter.putRow([.1]);
        putter.putRow([.01]);
        putter.putRow([10.0]);
        await putter.flush();
        await putter.close();
    });

    step('Fetch NUMERIC value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        let val = res.data.shift()[keys[0]];
        expect(val.toString()).to.equal("12.34");
        val = res.data.shift()[keys[0]];
        expect(val.toString()).to.equal("1.0");
        val = res.data.shift()[keys[0]];
        expect(val.toString()).to.equal("-1.0");
        val = res.data.shift()[keys[0]];
        expect(val.toString()).to.equal("0.0");
        val = res.data.shift()[keys[0]];
        expect(val.toString()).to.equal("0.1");
        val = res.data.shift()[keys[0]];
        expect(val.toString()).to.equal("0.01");
        val = res.data.shift()[keys[0]];
        expect(val.toString()).to.equal("10.0");
    });

    step('Insert invalid NUMERIC value', async function() {
        const res = await runQueryPromise(`INSERT INTO test values ('aaa')`);
        should.exist(res.err);
    });

    step('Clean up NUMERIC table', async function() {
        const res = await runQueryPromise('DROP TABLE test');
        should.not.exist(res.err);
    });
});

xdescribe('Connection', function() {

    step('Unsecured connection with unsecured worker', async function() {
        const sqream = new Connection({...config});
        let success = true;
        try {
            (await sqream.connect()).disconnect();
        } catch(e) {
            success = false;
        }
        expect(success, "Succesful connection").to.equal(true);
    });

    step('Unsecured connection with secured worker', async function() {
        const sqream = new Connection({...config, port: 5001});
        let success = false;
        try {
            (await sqream.connect()).disconnect();
        } catch(e) {
            success = true;
        }
        expect(success, "Unsuccesful connection").to.equal(true);
    });

    step('Secured connection with unsecured worker', async function() {
        const sqream = new Connection({...config, is_ssl: true});
        let success = false;
        try {
            (await sqream.connect()).disconnect();
        } catch(e) {
            success = true;
        }
        expect(success, "Unsuccesful connection").to.equal(true);
    });

    step('Secured connection with secured worker', async function() {
        const sqream = new Connection({...config, port: 5001, is_ssl: true});
        let success = true;
        try {
            (await sqream.connect()).disconnect();
        } catch(e) {
            success = false;
        }
        expect(success, "Succesful connection").to.equal(true);
    });

    step('Unsecured connection with unsecured load balancer', async function() {
        const sqream = new Connection({...config, port: 3108, cluster: true});
        let success = true;
        try {
            (await sqream.connect()).disconnect();
        } catch(e) {
            success = false;
        }
        expect(success, "Succesful connection").to.equal(true);
    });

    step('Unsecured connection with secured load balancer', async function() {
        const sqream = new Connection({...config, port: 3109, cluster: true});
        let success = false;
        try {
            (await sqream.connect()).disconnect();
        } catch(e) {
            success = true;
        }
        expect(success, "Unsuccesful connection").to.equal(true);
    });

    step('Secured connection with unsecured load balancer', async function() {
        const sqream = new Connection({...config, port: 3108, is_ssl: true, cluster: true});
        let success = false;
        try {
            (await sqream.connect()).disconnect();
        } catch(e) {
            success = true;
        }
        expect(success, "Unsuccesful connection").to.equal(true);
    });

    step('Secured connection with secured load balancer', async function() {
        const sqream = new Connection({...config, port: 3109, is_ssl: true, cluster: true});
        let success = true;
        try {
            (await sqream.connect()).disconnect();
        } catch(e) {
            success = false;
        }
        expect(success, "Succesful connection").to.equal(true);
    });

});

describe('Features', function() {
    step('sqlSanitize', async function() {
        let sql = Connection.sqlSanitize("SELECT %i FROM public.%i WHERE name = %s AND num > %d AND active = %b", [
            "col1", "table2", "john's", 50, true
        ]).statements[0];
        expect(sql).to.eql(`SELECT col1 FROM public.table2 WHERE name = 'john''s' AND num > 50 AND active = TRUE`);

        sql = Connection.sqlSanitize("%i", [`test 1`]).statements[0];
        expect(sql).to.eql(`"test 1"`);

        sql = Connection.sqlSanitize("%i", [`"test"`]).statements[0];
        expect(sql).to.eql(`"""test"""`);

        sql = Connection.sqlSanitize("select 'select 1;'; select 2;", []);
        expect(sql.statements[0]).to.eql(`select 'select 1;'`);
        expect(sql.statements[1]).to.eql(`select 2`);
    });

    step('Connect', async function() {
        const sqream = new Connection(config);

        const conn = await sqream.connect();
        let res = await conn.execute("select 1");
        expect(res.length).to.eql(1);

        res = await conn.execute("select 2");
        expect(res.length).to.eql(1);

        let closed = false;
        conn.onClose.then(() => closed = true);
        await conn.disconnect();
        expect(closed).to.eql(true);
    });

    step('executeCursor', async function() {
        const conn = new Connection(config);

        await conn.execute("CREATE OR REPLACE TABLE test (bool_column bool)");
        await conn.execute("INSERT INTO test VALUES (false), (false), (false), (true)");

        let cursor = await conn.executeCursor("SELECT * FROM test");
        let first = null;
        let last = null;
        let count = 0;
        try {
            for await(let rows of cursor.fetchIterator(2)) {
                count += rows.length;
                if (first === null) first = rows[0].bool_column;
                expect(rows.length).to.eql(2);
                last = rows.pop().bool_column;
            }
        } catch (e) {
            await cursor.close();
            throw e;
        }

        await cursor.close();
        await conn.execute('DROP TABLE test');

        expect(count).to.eql(4);
        expect(first).to.eql(0);
        expect(last).to.eql(1);
    });

    step('Get Protocol Version', async function() {
        const sqream = new Connection(config);

        const conn = await sqream.connect();
        expect(conn.getClientProtocolVersion()).to.be.gt(5);
        expect(conn.getServerProtocolVersion()).to.be.gt(5);

        await conn.disconnect();
    });

    // @TODO: test is too flaky
    // step('networkTimeout', async function() {
    //     const conn = new Connection({...config, networkTimeout: 30});
    //     let message = null;
    //     try {
    //         await conn.execute("select 1");
    //     } catch (e) {
    //         message = e.message
    //     }
    //     expect(message).to.be.a('string');
    //     expect(message.toLowerCase()).to.include('timed out');
    // });
});
