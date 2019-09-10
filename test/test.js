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

// const query1 = "SELECT 1 as test, 2 as other_test;";
  const query1 = "SELECT 'koko'";
 //const query1 = "set sessionTag='webui';";
//const query1 = "set developerMode=true;";
// const query1 = "SELECT get_table_types()";
// const query1 = "select * from sqream_catalog.tables;";
// const query1 = "select * from nation;";
//const setFlag = "SET sessionTag='webui2';";
// const setFlag = "set sessionTag='webui';";
const setFlag = null;

const myConnection = new Connection(config);

myConnection.events.on('getConnectionId', function(data){
   // console.log('getConnectionId', data);
});

myConnection.events.on('getStatementId', function(data){
    // console.log('getStatementId', data);
});

myConnection.events.on('getTypes', function(data){
    // console.log('getTypes', data);
});


myConnection.runQuery(query1, function (err, data){
  console.log('===== Results ==== ');
  console.log(err, data);
}, setFlag );
