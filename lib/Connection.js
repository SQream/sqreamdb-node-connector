// @ts-check
"use strict";

const EventEmitter = require('events');
const sqConnect = require('./SqConnect');
const sanitize = require('./SqlSanitize');
const { createInsertBuffer, SqNumeric } = require('./SqreamDataWriter');
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
    const networkTimeout = !config.networkTimeout ? 180000 : +config.networkTimeout;
    const connectionTimeout = !config.connectionTimeout ? 3000 : +config.connectionTimeout;
    let conn;
    let version;

    let closeRes;

    const onClose = new Promise((res) => {
      closeRes = res;
    });
    
    try {
      conn = await sqConnect(host, port, !config.cluster && is_ssl, debug, connectionTimeout);
      conn.onError(closeRes);
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
        conn = await sqConnect(host, port, is_ssl, debug, connectionTimeout);
        conn.onError(closeRes);
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
    version = connectionResponse.version;

    let queryChain = Promise.resolve();
    let doNotEmit = false;

    const emit = (subject, data) => {
      if (doNotEmit) return;
      events.emit(subject, data);
    }
    
    const query = async (statement, ...replacements) => {
      const sqls = sqlSanitize(statement, replacements);
      const sql = sqls.statements.shift();
      if (!sql) return Promise.reject(new Error("No sql defined"));
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

      if (versionCompare(version, "2020.3.1") > -1) {
        const int = setInterval(() => {
          conn.send({ping: 'ping'})
        }, 10000);
    
        queryChain.then(() => clearInterval(int));
      }
  
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
              conn = await sqConnect(ipResponse.ip, is_ssl ? ipResponse.port_ssl : ipResponse.port, is_ssl, debug, connectionTimeout);
              conn.onError(closeRes);
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
          done();
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
            const queryTypeInResponse = await conn.once("queryType", networkTimeout);
            queryObj.queryTypeIn = queryTypeInResponse.queryType;
        
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

            function put() {
              if (!queryObj.queryTypeIn || !queryObj.queryTypeIn.length) {
                throw new Error("Query is not parameterized with question marks");
              }
              let byteCount = 0;
              const putter = createInsertBuffer(queryObj.queryTypeIn, queryObj.varchar_encoding);
              let flushDone = Promise.resolve();
              const flush = () => {
                return flushDone = (async () => {
                  const rowCount = putter.getRowCount();
                  byteCount += putter.getByteCount();
                  const buf = putter.getBuffer();
                  await flushDone;
                  await conn.send({"put": rowCount}, buf);
                  await conn.once('putted', networkTimeout);
                })();
              }
              
              return {
                columns: queryObj.queryTypeIn,
                putRow: async (row) => {
                  if (!putter.setRow(row)) {
                    await flush();
                    return true;
                  }
                  return false;
                },
                getByteCount: () => byteCount,
                flush: flush,
                close: async () => {
                  putter.cancel();
                  conn.send({closeStatement: "closeStatement"});
                  await conn.once("statementClosed", networkTimeout).finally(done)
                }
              };
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
              put,
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

    const executeInsert = async (sql, ...replacements) => {
      const ready = await query(sql, ...replacements);
      const queued = await ready.enqueue();
      try {
        const exec = await queued.execute();
        return exec.put();
      } catch (e) {
        await queued.close();
        throw e;
      }
    }

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
      executeInsert,
      getClientProtocolVersion: conn.getClientProtocolVersion,
      getServerProtocolVersion: conn.getServerProtocolVersion,
      getSqreamVersion
    }

    function closeConn(reason) {
      conn.close(reason);
      closeRes();
    }

    function getSqreamVersion() {
      return version;
    }
  }

  async executeInsert(sql, ...replacements) {
    const conn = await this.connect();
    try {
      const putter = await conn.executeInsert(sql, ...replacements);
      return {
        ...putter,
        close: async () => {
          return putter.close().finally(conn.disconnect);
        }
      }
    } catch (err) {
      conn.disconnect();
      throw err;
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

  async executeMany(statement, ...placeholders) {
    const sqls = sqlSanitize(statement, placeholders).statements.filter(Boolean);
    let res;
    const conn = await this.connect();
    try {
      for (let sql of sqls) {
        res = await conn.execute(sql);
      }
    } finally {
      conn.disconnect();
    }
    return res;
  }

  // Backwards compatibility
  runQuery(sql, callback, setCommand) {
    this.connect().then((conn) => {
      if (setCommand) {
        conn.execute(setCommand).then((data) => {
          conn.execute(sql).then((data) => {
            conn.disconnect();
            callback(undefined, data);
          }, (err) => {
            conn.disconnect();
            callback(err, undefined);
          });
        }, (err) => {
          conn.disconnect();
          callback(err, undefined);
        });
      } else {
        conn.execute(sql).then((data) => {
          conn.disconnect();
          callback(undefined, data);
        }, (err) => {
          conn.disconnect();
          callback(err, undefined);
        });
      }
    }, (err) => {
      callback(err, undefined);
    });
  }

} //EOC

let versionReg = /\d{4}(\.\d+)*/;
let verisonTrimReg = /(\.0)+$/;

/**
 * 
 * @param {string} v1 
 * @param {string} v2 
 * @returns 
 */
function versionCompare(v1, v2) {
  if (!v2 || !v1) return undefined;
  let orig = v1;
  v1 = (("" + v1).match(versionReg) || [])[0];
  v2 = (("" + v2).match(versionReg) || [])[0];
  if (!v1 || !v2) return undefined;
  v1 = v1.replace(verisonTrimReg, "");
  v2 = v2.replace(verisonTrimReg, "");
  if (orig.match(/_/)) {
    v1 += '.1';
  }
  return v1.localeCompare(v2, 'en', {numeric: true});
}

Connection.sqConnect = sqConnect;
Connection.sqlSanitize = sanitize.sqlSanitize;
Connection.extractStrings = sanitize.extractStrings;
Connection.SqNumeric = SqNumeric;
Connection.versionCompare = versionCompare;

module.exports = Connection;
