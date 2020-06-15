# Connecting SQream with nodejs

## Requirement

1. Copy sqream-node-connector to sub folder on your project.
2. npm install <sqream-connector-{version}.tgz>


### Create a sample connection for testing

Below is a sample nodejs file you can use to test your connection. <br />
Make sure to edit the required details, such as:
 * your server address
 * your server port
 * your username
 * your password
 * cluster: true/false
 * is_ssl: true/false
 * service: service name

```javascript
// filename: test_sqream.js
const Connection = require('sqream-connector');

const config = {
  host: '<your server address>',
  port: 5000,
  username: '<your username>',
  password: '<your password>',
  connectDatabase: '<your database>',
};

const query1 = "SELECT 1 as test, 2 as other_test";
const sqream = new Connection(config);
sqream.execute(query1).then((data) => {
  console.log(data);
}, (err) => {
  console.error(err);
});

```


Run your file with node:

```bash
node test_sqream.js
```

Config with cluster

```javascript
const config = {
  host: '<your server address>',
  port: 3108,
  username: '<your username>',
  password: '<your password>',
  connectDatabase: '<your database>',
  cluster: true,
};

```

Secure connection

```javascript
const config = {
  host: '<your server address>',
  port: 5100,
  username: '<your username>',
  password: '<your password>',
  connectDatabase: '<your database>',
  is_ssl: true
};

```

On a successful connection, you should see:

```
[ { test: 1, other_test: 2 } ]
```

Otherwise, please contact SQream support with the error description.


## Limitation

The node application which utilizes the sqream connector should consider the heap size node configuration.

 When processing large datasets,It is recommended to increase the application heap size with the `--max_old_space_size` node run flag:

node --max_old_space_size={heapSize} my-application.js

 In addition,

The following error mentions that the heap size configured is not sufficient:

FATAL ERROR: CALL_AND_RETRY_LAST Allocation failed - JavaScript heap out of memory
