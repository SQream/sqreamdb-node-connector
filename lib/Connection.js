// @ts-check
"use strict";

const EventEmitter = require('events');
const sqConnect = require('./SqConnect');
const sanitize = require('./SqlSanitize');
const sqlSanitize = sanitize.sqlSanitize;

function getParamType(value) {
  let to = typeof value;
  if(to === 'boolean') {
    return 'b';
  }
  if (to === 'number') {
    return 'd';
  }
  return 's';
}

const MAX_ARRAY_LENGTH = 2 ** 32 - 1;

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
    const networkTimeout = config.networkTimeout === undefined ? 180000 : +config.networkTimeout;
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
    const connected = conn.once("databaseConnected", networkTimeout);
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
        statementResponse = await conn.once("statementId", networkTimeout);
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
              conn = await sqConnect(ipResponse.ip, is_ssl ? ipResponse.port_ssl : ipResponse.port, is_ssl, debug);
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
            const databaseResponse = await conn.once("databaseConnected", networkTimeout);
            queryObj.varchar_encoding = databaseResponse.varcharEncoding;
            conn.send({"reconstructStatement": queryObj.statement_id});
            try {
              await conn.once("statementReconstructed", networkTimeout);
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
              await conn.once("executed", 0);
            } catch (e) {
              throw new Error("Error executing statement: " + e.message);
            }
        
            conn.send({"queryTypeIn": "queryTypeIn"});
            await conn.once("queryType", networkTimeout);
        
            conn.send({"queryTypeOut": "queryTypeOut"});
            const queryTypeReponse = await conn.once("queryTypeNamed", networkTimeout);

            queryObj.queryTypeNamed = queryTypeReponse.queryTypeNamed;
            emit('getTypes', queryTypeReponse.queryTypeNamed);

            async function* iterator(chunkSize = 10000) {
              if (queryObj.queryTypeNamed.length === 0) {
                return;
              }
              let done = false;
              let remainder = [];
              let chunks = [];
              let error;
              conn.subscribe("data", (data) => {
                const appendToRemainder = Math.min(chunkSize - remainder.length, data.length);
                remainder = remainder.concat(data.slice(0, appendToRemainder));
                if (remainder.length !== chunkSize) {
                  return;
                }
                chunks.push(remainder);
                remainder = [];
                for (let i = chunkSize + appendToRemainder; i <= data.length; i += chunkSize) {
                  chunks.push(data.slice(i - chunkSize, i));
                }
                if ((data.length - appendToRemainder) % chunkSize) {
                  remainder = data.slice(-1 * ((data.length - appendToRemainder) % chunkSize));
                }
              });
              conn.once("done", 0).then(() => {
                if (remainder.length) {
                  chunks.push(remainder);
                }
                remainder = [];
                done = true;
              }, (err) => {
                error = err;
              });

              while (!done || chunks.length) {
                if (error) throw error;
                if (chunks.length) {
                  yield chunks.shift();
                } else {
                  conn.send({"fetch": "fetch"});
                  await conn.once('data done', 0);
                }
              }
            }

            return {
              queryTypeNamed: queryObj.queryTypeNamed,
              fetchIterator: iterator,
              fetchAll: async (rowLimit = MAX_ARRAY_LENGTH) => {
                rowLimit = +rowLimit;
                if (rowLimit <= 0 || rowLimit > MAX_ARRAY_LENGTH) {
                  rowLimit = MAX_ARRAY_LENGTH;
                }
                let chunks = [];
                let rowCount = 0;
                conn.setMaxRows(rowLimit);
                for await (let chunk of iterator()) {
                  chunks.push(chunk);
                  rowCount += chunk.length;
                  if (rowCount >= rowLimit) break;
                  conn.setMaxRows(rowLimit - rowCount);
                }
                conn.setMaxRows(MAX_ARRAY_LENGTH);
                const rows = [].concat.apply([], chunks).slice(0, rowLimit);
                chunks.length = 0;
                return rows;
              },
              close: async ()=> {
                conn.send({closeStatement: "closeStatement"});
                return conn.once("statementClosed", networkTimeout).finally(done)
              }
            };
          } catch (err) {
            conn.send({closeStatement: "closeStatement"});
            try {
              await conn.once("statementClosed", networkTimeout);
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
      try {
        const fetched = await executed.fetchAll()
        await executed.close();
        return fetched;
      } catch (e) {
        await executed.close();
        throw e;
      }
    };

    const executeCursor = async (sql, ...placeholders) => {
      const ready = await query(sql, ...placeholders);
      const queued = await ready.enqueue();
      return await queued.execute();
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
      execute,
      executeCursor,
      getClientProtocolVersion: conn.getClientProtocolVersion,
      getServerProtocolVersion: conn.getServerProtocolVersion
    }

    function closeConn(reason) {
      conn.close(reason);
      closeRes();
    }
  }

  async executeCursor(sql, ...replacements) {
    const conn = await this.connect();
    try {
      const res = await conn.executeCursor(sql, ...replacements);
      return {
        ...res,
        close: async () => {
          return res.close().finally(conn.disconnect);
        }
      }
    } catch (err) {
      conn.disconnect();
      throw err;
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
