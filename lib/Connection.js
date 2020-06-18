// @ts-check
"use strict";

const EventEmitter = require('events');
const sqConnect = require('./SqConnect');
const sanitize = require('./SqlSanitize');
const sqlSanitize = sanitize.sqlSanitize;

function getParamType(value) {
  let to = typeof value;
  if(to === 'boolean') {
    return 'b'
  }
  if (to === 'number') {
    return 'd'
  }
  return 's';
}

class Connection {

  constructor(config) {
    this.config = config;
    this.events = new EventEmitter();
  }

  async connect(sessionParams = {}) {
    const config = this.config;
    let host = config.host;
    let port = config.port;
    const debug = this.config.debug || false;
    const is_ssl = config.is_ssl;
    const events = this.events;
    let conn;

    let closeRes;
    let closeRej;

    const onClose = new Promise((res, rej) => {
      closeRes = res;
      closeRej = rej;
    });
    
    try {
      conn = await sqConnect(host, port, !config.cluster && is_ssl, debug);
      conn.onError(closeRej);
    } catch (e) {
      const type = config.cluster ? 'serverpicker' : 'sqreamd';
      throw new Error(`Error connecting to ${type}: ${e.message}`);
    }

    if (config.cluster) {
      let buf;
      try {
        buf = await conn.firstBuffer();
        if (buf.error) {
          throw new Error(buf.error);
        }
      } catch (e) {
        throw new Error("Error connecting to serverpicker: " + e.message);
      }
      closeConn("Expected close");
      const ipSize = buf.readInt8(0);
      host = buf.slice(4, 4 + ipSize).toString('ascii');
      port = buf.readInt16LE(4 + ipSize);
      try {
        conn = await sqConnect(host, port, is_ssl, debug);
        conn.onError(closeRej);
      } catch (e) {
        throw new Error(`Error connecting to sqreamd: ${e.message}`);
      }
    }
    const queryObj = {host: config.host, port: config.port, worker: {host, port}};
    const connected = conn.once("databaseConnected");
    conn.send({
      username: (config.username || "").toString(),
      password: (config.password || "").toString(),
      connectDatabase: (config.connectDatabase || "").toString(),
      service: (config.service || "").toString() || 'sqream'
    });
    let connectionResponse;
    try {
      connectionResponse = await connected;
      if (connectionResponse.error) {
        throw new Error(connectionResponse.error);
      }
    } catch (e) {
      throw new Error("Error connecting to sqreamd: " + e.message);
    }
    
    events.emit('getConnectionId', {
      host: queryObj.host,
      port: queryObj.port,
      connectionId: connectionResponse.connectionId
    });

    queryObj.varchar_encoding = connectionResponse.varcharEncoding;
    queryObj.connectionId = connectionResponse.connectionId;

    let queryChain = Promise.resolve();
    let doNotEmit = false;

    const emit = (subject, data) => {
      if (doNotEmit) return;
      events.emit(subject, data);
    }
    
    const query = async (statement, ...replacements) => {
      const sqls = sqlSanitize(statement, replacements);
      const sql = sqls.statements.pop();
      if (!sql) return Promise.reject(new Error("No sql defined"));
      for(let sql of sqls.statements) {
        await execute(sql);
      }
      const last = queryChain;
      let done = () => {};
      queryChain = new Promise((resolve) => {
        done = () => {
          conn.clear();
          resolve();
        };
      });
      await last;
      conn.send({"getStatementId": "getStatementId"});
  
      let statementResponse;
      try {
        statementResponse = await conn.once("statementId", 20000);;
      } catch (e) {
        done();
        throw new Error("Error getting statement id: " + e.message);
      }
      
      const outData = {
        host: queryObj.host,
        port: queryObj.port,
        statement_id: statementResponse.statementId
      };
      emit('getStatementId', outData);
  
      queryObj.statement_id = statementResponse.statementId;

      let alreadyEnqueued = false;
      const enqueue = async () => {
        if (alreadyEnqueued) return Promise.reject(new Error("Already enqueued"));
        alreadyEnqueued = true;
        try {
          conn.send({"chunkSize": 0, "prepareStatement": sql});
          let res;
          try {
            res = await conn.once("ip", 0);
          } catch (e) {
            throw new Error("Error preparing statement: " + e.message);
          }

          if (res.error) {
            throw new Error(res.error);
          }
      
          const ipResponse = res;
      
          if (ipResponse.reconnect) {
            queryObj.worker = {host: ipResponse.ip, port: ipResponse.port};
            closeConn("Expected close");
            try {
              conn = await sqConnect(ipResponse.ip, is_ssl ? ipResponse.port_ssl :ipResponse.port, is_ssl, debug);
              conn.onError(closeRej);
            } catch (e) {
              throw new Error(`Error connecting to sqreamd: ${e.message}`);
            }
            conn.send({
              username: (config.username || "").toString(),
              password: (config.password || "").toString(),
              reconnectDatabase: (config.connectDatabase || "").toString(),
              service: (config.service || "").toString() || 'sqream',
              connectionId: queryObj.connectionId,
              listenerId: ipResponse.listener_id
            });
            const databaseResponse = await conn.once("databaseConnected");
            queryObj.varchar_encoding = databaseResponse.varcharEncoding;
            conn.send({"reconstructStatement": queryObj.statement_id});
            try {
              await conn.once("statementReconstructed");;
            } catch (e) {
              throw new Error("Error reconstructing statement: " + e.message);
            }
          }
        } catch(err) {
          conn.send({closeStatement: "closeStatement"});
          try {
            await conn.once("statementClosed", 0);
          } catch(e) {}
          throw err;
        }
        let alreadyExeced = false;
        const exec = async () => {
          if (alreadyExeced) return Promise.reject(new Error("Already executed"));
          alreadyExeced = true;
          try {
            conn.send({"execute": "execute"});
            try {
              await conn.once("executed", 0);;
            } catch (e) {
              throw new Error("Error executing statement: " + e.message);
            }
        
            conn.send({"queryTypeIn": "queryTypeIn"});
            await conn.once("queryType");;
        
            conn.send({"queryTypeOut": "queryTypeOut"});
            const queryTypeReponse = await conn.once("queryTypeNamed");

            queryObj.queryTypeNamed = queryTypeReponse.queryTypeNamed;
            emit('getTypes', queryTypeReponse.queryTypeNamed);

          
            let fetchPromise = null;

            if (queryObj.queryTypeNamed.length === 0) {
              fetchPromise = Promise.resolve([]);
            }
            let fetchDone = false;
            const fetchAll = (rowLimit = Number.MAX_SAFE_INTEGER) => {
              if (fetchPromise) return fetchPromise;
              return fetchPromise = new Promise((fr, fetchrej) => {
                let rows = [];
                const fetchres = () => {
                  fr(rows.slice(0, rowLimit))
                  rows.length = 0;
                }
                conn.subscribe("data", (data) => {
                  if (fetchDone) return;
                  const start = rows.length;
                  const diff = rows.length + data.length - rowLimit;
                  const dataLen = diff > 0 ? data.length - diff : data.length;
                  rows.length = rows.length + dataLen;
                  for (let i = 0; i < dataLen; i += 100000) {
                    const max = i + 100000;
                    for (let j = i; j < dataLen && j < max; j++) { // loop tiling optimization
                      rows[start + j] = data[j];
                    }
                  }
                  data.length = 0;
                  if (rows.length >= rowLimit) {
                    fetchDone = true;
                    fetchres();
                  } else {
                    conn.setMaxRows(rowLimit - rows.length);
                    conn.send({"fetch": "fetch"});
                  }
                });
                conn.once("done", 0).then(() => {
                  if (fetchDone) return;
                  fetchDone = true;
                  fetchres();
                }).catch(fetchrej);
                conn.setMaxRows(rowLimit);
                conn.send({"fetch": "fetch"});
              });
            }
            return {
              queryTypeNamed: queryObj.queryTypeNamed,
              fetchAll: async(rowLimit = Number.MAX_SAFE_INTEGER) => {
                return {
                  results: await fetchAll(rowLimit),
                  close: async ()=> {
                    conn.send({closeStatement: "closeStatement"});
                    return conn.once("statementClosed", 60000).finally(done)
                  }
                };
              }
            };
          } catch (err) {
            conn.send({closeStatement: "closeStatement"});
            try {
              await conn.once("statementClosed", 60000);
            } catch(e) {}
            throw err;
          }
        }
        return {
          worker: queryObj.worker,
          varcharEncoding: queryObj.varchar_encoding,
          execute: () => {
            let p = exec();
            p.catch(done);
            return p;
          }
        };
      }
      
      return {
        statementId: statementResponse.statementId,
        sql,
        enqueue: () => {
          let p = enqueue();
          p.catch(done);
          return p;
        }
      }
    }

    const execute = async (sql, ...placeholders) => {
      const ready = await query(sql, ...placeholders);
      const queued = await ready.enqueue();
      const executed = await queued.execute();
      const fetched = await executed.fetchAll()
      await fetched.close();
      return fetched.results;
    };

    doNotEmit = true;
    try {
      for (let param in sessionParams) {
        await execute(`SET ${param.replace(/[^\w]/g, "")}=%${getParamType(sessionParams[param])}`, sessionParams[param]);
      }
    } catch (err) {
      closeConn("Session params error");
      doNotEmit = false;
      throw err;
    }
    doNotEmit = false;
    
    return {
      worker: {host: queryObj.host, port: queryObj.port},
      connectionId: queryObj.connectionId,
      varcharEncoding: queryObj.varchar_encoding,
      disconnect: () => {
        closeConn("Manual close");
      },
      onClose,
      query,
      execute
    }

    function closeConn(reason) {
      conn.close(reason);
      closeRes();
    }
  }

  async execute(sql, ...replacements) {
    const conn = await this.connect();
    try {
      const res = await conn.execute(sql, ...replacements);
      conn.disconnect();
      return res;
    } catch (err) {
      conn.disconnect();
      throw err;
    }
  }

  // Backwards compatibility
  runQuery(sql, callback, setCommand) {
    setCommand = setCommand ? setCommand + ";" : "";
    this.execute(setCommand + sql).then((data) => {
      callback(undefined, data);
    }, (err) => {
      callback(err, undefined);
    });
  }

} //EOC

Connection.sqConnect = sqConnect;
Connection.sqlSanitize = sanitize.sqlSanitize;
Connection.extractStrings = sanitize.extractStrings;

module.exports = Connection;
