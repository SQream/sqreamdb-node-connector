# Connecting SQream with nodejs

## Requirements

- NodeJS 10.x or newer

## Installation

`npm install @sqream/sqreamdb`

## Create a sample connection for testing

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
const Connection = require('@sqream/sqreamdb');

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

On a successful connection, you should see:

```javascript
[ { test: 1, other_test: 2 } ]
```

## Config with cluster

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

## Secure connection

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

## Lazyloading

If you want to process rows without keeping them in memory, you can lazyload the rows:

```javascript
const query2 = "SELECT * FROM public.large_table";
const sqream = new Connection(config);

(async () => {
  const cursor = await sqream.executeCursor(query1);
  let count = 0;
  for await (let rows of curser.fetchIterator(100)) { // fetch rows in chunks of 100
    count += rows.length;
  }
  await cursor.close();
  return conut;
}).then((total) => {
  console.log('Total rows', total);
}, (err) => {
  console.error(err);
});
```

## Keeping an open connection

```javascript
const sqream = new Connection(config);

(async () => {
  const conn = await sqream.connect();
  try {
    const res1 = await conn.execute("SELECT 1");
    const res2 = await conn.execute("SELECT 2");
    const res3 = await conn.execute("SELECT 3");
    conn.disconnect();
    return {res1, res2, res3};
  } catch (err) {
    conn.disconnect();
    throw err;
  }
}).then((total) => {
  console.log('Results', res)
}, (err) => {
  console.error(err);
});
```

## Variable placeholders

Variable placeholders allows to have queries that include dynamic values, like user input, by placing placeholders in the query which get replaced with escaped values that are supplied to the function.

```javascript
const sqream = new Connection(config);
const sql = "SELECT %i FROM public.%i WHERE name = %s AND num > %d AND active = %b";
sqream.execute(sql, "col1", "table2", "john's", 50, true)'
```

The above sql gets converted internally to the following:


```
SELECT "col1" FROM public."table2" WHERE name = 'john''s' AND num > 50 AND active = TRUE
```

The different types of placeholders:

- `%i` - identifier
- `%s` - string
- `%d` - number
- `%b` - boolean

## Limitation

The node application which utilizes the sqream connector should consider the heap size node configuration.

 When processing large datasets, it is recommended to increase the application heap size with the `--max_old_space_size` node run flag:

```
node --max_old_space_size={heapSize} my-application.js
```

In addition, the following error mentions that the heap size configured is not sufficient:

`FATAL ERROR: CALL_AND_RETRY_LAST Allocation failed - JavaScript heap out of memory`
