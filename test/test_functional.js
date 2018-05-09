const should = require('chai').should();
const expect = require('chai').expect;
const Connection = require('../index');

const config = {
    host: '127.0.0.1',
    port: 5000,
    username: 'sqream',
    password: 'sqream',
    connectDatabase: 'master',
    cluster: false,
    is_ssl: false
};

function runQuery(query, cb) {
    const myConnection = new Connection(config);
    myConnection.runQuery(query, function (err, data){
        cb(err, data)
    }, null );
}


function runQueryPromise(query) {
    return new Promise(function (resolve) {
        const myConnection = new Connection(config);
        myConnection.runQuery(query, function (err, data){
            resolve({ err: err, data: data });
        }, null );
    })
}

describe('Boolean Table', function() {
    step('Create Bool Table', async function() {
        const res = await runQueryPromise('CREATE OR REPLACE TABLE test (bool_column bool)');
        should.not.exist(res.err);
    });

    step('Insert into Bool Table', async function() {
        const res = await runQueryPromise('INSERT INTO test VALUES (false)');
        should.not.exist(res.err);
    });

    step('Fetch Bool value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        expect(val).to.equal(0);
    });

    step('Insert into Bool Table - negative test', async function() {
        const res = await runQueryPromise('insert into test values (40000)');
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
        const res = await runQueryPromise('INSERT INTO test VALUES (1)');
        should.not.exist(res.err);
    });

    step('Fetch TINYINT value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        expect(val).to.equal(1);
    });

    step('Insert into TINYINT Table - negative test', async function() {
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
        const res = await runQueryPromise('INSERT INTO test VALUES (1)');
        should.not.exist(res.err);
    });

    step('Fetch SMALLINT value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        expect(val).to.equal(1);
    });

    step('Insert into SMALLINT Table - negative test', async function() {
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
        const res = await runQueryPromise('INSERT INTO test VALUES (1)');
        should.not.exist(res.err);
    });

    step('Fetch INT value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        expect(val).to.equal(1);
    });

    step('Insert into INT Table - negative test', async function() {
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
        const res = await runQueryPromise('INSERT INTO test VALUES (1)');
        should.not.exist(res.err);
    });

    step('Fetch BIGINT value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        expect(val).to.equal('1');
    });

    step('Insert into BIGINT Table - negative test', async function() {
        const res = await runQueryPromise("INSERT INTO test values ('test')");
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
        const res = await runQueryPromise('INSERT INTO test VALUES (1.0)');
        should.not.exist(res.err);
    });

    step('Fetch FLOAT value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        expect(val).to.equal(1.0);
    });

    step('Insert into FLOAT Table - negative test', async function() {
        const res = await runQueryPromise("INSERT INTO test values ('test')");
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
        const res = await runQueryPromise('INSERT INTO test VALUES (1.0)');
        should.not.exist(res.err);
    });

    step('Fetch REAL value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        expect(val).to.equal(1.0);
    });

    step('Insert into REAL Table - negative test', async function() {
        const res = await runQueryPromise("INSERT INTO test values ('test')");
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
        const res = await runQueryPromise("INSERT INTO test VALUES ('2010-10-10')");
        should.not.exist(res.err);
    });

    step('Fetch DATE value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        expect(val).to.equal("2010-10-10");
    });

    step('Insert into DATE Table - negative test', async function() {
        const res = await runQueryPromise('INSERT INTO test values (false)');
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
        const res = await runQueryPromise("INSERT INTO test VALUES ('2010-10-10 23:59:59')");
        should.not.exist(res.err);
    });

    step('Fetch DATETIME value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        expect(val).to.equal("2010-10-10 23:59:59.000");
    });

    step('Insert into DATETIME Table - negative test', async function() {
        const res = await runQueryPromise('INSERT INTO test values (false)');
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
        const res = await runQueryPromise("INSERT INTO test VALUES ('2010-10-10')");
        should.not.exist(res.err);
    });

    step('Fetch TIMESTAMP value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        expect(val).to.equal("2010-10-10 00:00:00.000");
    });

    step('Insert into TIMESTAMP Table - negative test', async function() {
        const res = await runQueryPromise('INSERT INTO test values (false)');
        should.exist(res.err);
    });

    step('Clean up TIMESTAMP table', async function() {
        const res = await runQueryPromise('DROP TABLE test');
        should.not.exist(res.err);
    });
});

describe('VARCHAR Table', function() {
    step('Create VARCHAR Table', async function() {
        const res = await runQueryPromise('CREATE OR REPLACE TABLE test (t varchar(20))');
        should.not.exist(res.err);
    });

    step('Insert into VARCHAR Table', async function() {
        const res = await runQueryPromise("INSERT INTO test VALUES ('test')");
        should.not.exist(res.err);
    });

    step('Fetch VARCHAR value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        expect(val.trim()).to.equal("test");
    });

    step('Insert into VARCHAR Table - negative test', async function() {
        const res = await runQueryPromise("INSERT INTO test values ('this is more than just 20 characters, no need to count')");
        should.exist(res.err);
    });

    step('Clean up VARCHAR table', async function() {
        const res = await runQueryPromise('DROP TABLE test');
        should.not.exist(res.err);
    });
});

describe('NVARCHAR Table', function() {
    step('Create NVARCHAR Table', async function() {
        const res = await runQueryPromise('CREATE OR REPLACE TABLE test (t varchar(20))');
        should.not.exist(res.err);
    });

    step('Insert into NVARCHAR Table', async function() {
        const res = await runQueryPromise("INSERT INTO test VALUES ('test')");
        should.not.exist(res.err);
    });

    step('Fetch NVARCHAR value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        expect(val.trim()).to.equal("test");
    });

    step('Insert into NVARCHAR Table - negative test', async function() {
        const res = await runQueryPromise("INSERT INTO test values ('this is more than just 20 characters, no need to count')");
        should.exist(res.err);
    });

    step('Clean up NVARCHAR table', async function() {
        const res = await runQueryPromise('DROP TABLE test');
        should.not.exist(res.err);
    });
});

describe('NULL value check', function() {
    step('Create nullable int Table', async function() {
        const res = await runQueryPromise('CREATE OR REPLACE TABLE test (t int null)');
        should.not.exist(res.err);
    });

    step('Insert into int Table', async function() {
        const res = await runQueryPromise("INSERT INTO test VALUES (null)");
        should.not.exist(res.err);
    });

    step('Fetch NULL value', async function() {
        const res = await runQueryPromise('SELECT * FROM test');
        const keys =  Object.keys(res.data[0]);
        const val = res.data[0][keys[0]];
        expect(val).to.equal(null);
    });

	step('Create non nullable int Table', async function() {
        const res = await runQueryPromise('CREATE OR REPLACE TABLE test (t int not null)');
        should.not.exist(res.err);
    });

    step('Insert null value into int Table - negative test', async function() {
        const res = await runQueryPromise("INSERT INTO test VALUES (null)");
        should.exist(res.err);
    });

    step('Clean up NVARCHAR table', async function() {
        const res = await runQueryPromise('DROP TABLE test');
        should.not.exist(res.err);
    });
});
