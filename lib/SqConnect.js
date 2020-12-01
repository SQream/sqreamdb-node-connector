// @ts-check
"use strict";

const net = require('net');
const tls = require('tls');
const { messageHeader } = require('./HandleMessage');
const handleMessage = require('./HandleMessage');

function time() {
  const currentDate = new Date();
  return currentDate.getHours().toString().padStart(2, "0") + ":"
  + currentDate.getMinutes().toString().padStart(2, "0") + ":"
  + currentDate.getSeconds().toString().padStart(2, "0") + "."
  + currentDate.getMilliseconds().toString().padStart(3, "0");
}

module.exports = function sqConnect(host, port = 5000, is_ssl = false, debug = false) {
  if (typeof host != 'string' || typeof port != 'number') return Promise.reject(new Error(`Bad host or port: ${host}:${port}`));
  const client = is_ssl ? new tls.TLSSocket(undefined) : new net.Socket();
  if (debug) console.log(`[${time()}]-[sqream]-[connect]`, `${host}:${port}`);
  return new Promise((connRes, connRej) => {
    const submap = new Map();
    let subs = {};
    const errorSubs = new Set();
    let first = null;
    let firstRej = (reason) => {};
    let error = null;
    let alive = true;
    let maxRows = Number.MAX_SAFE_INTEGER;
    let serverProtocolVersion;

    function subscribe(type, callback) {
      if (!alive) return;
      const types = type.split(" ");
      const list = submap.get(callback) || new Set();
      types.forEach((type) => {
        subs[type] = subs[type] || new Set();
        subs[type].add(callback);
        list.add(type);
      });
      submap.set(callback, list);
      return {
        unsubscribe() {
          unsubscribe(callback);
        }
      }
    }

    function unsubscribe(callback) {
      const list = submap.get(callback);
      if (!list) return;
      list.forEach((type) => {
        if (typeof subs[type] === 'undefined') return;
        subs[type].delete(callback);
      });
      submap.delete(callback);
    }

    const onceSubs = new Set();

    function once(type, timeout = 20000) {
      if (!alive) return Promise.reject(new Error("Connection not alive"));
      return new Promise((onceRes, onceRej) => {
        let timer = null;
        const success = (res) => {
          clearTimeout(timer);
          unsubscribe(success);
          onceSubs.delete(failure);
          onceRes(res);
        }
        const failure = (err) => {
          clearTimeout(timer);
          unsubscribe(success);
          onceSubs.delete(failure);
          onceRej(err);
        }
        if (timeout > 0) {
          timer = setTimeout(() => {
            failure(new Error("Timed out waiting for '" + type + "' after " + timeout + "ms"));
          }, timeout);
        }
        onceSubs.add(failure);
        subscribe(type, success);
      });
    }

    function clear() {
      onceSubs.clear();
      subs = {};
      submap.clear();
      firstRej = () => {};
      first = null;
      bufferArray.length = 0;
      bufferLength = 0;
      isJson = true;
      colSzsMsg = {};
      colTypesMsg = {};
      varchar_encoding = "ascii";
    }

    function send(data, binary) {
      if (debug) console.log(`[${time()}]-[sqream]-[send]   `, data);
      client.write(handleMessage.jsonToBuffer(data));
      if (binary) {
        client.write(messageHeader(binary.length, true));
        client.write(binary);
      }
    }

    function onError(callback) {
      errorSubs.add(callback);
      return {
        unsubscribe() {
          errorSubs.delete(callback);
        }
      }
    }

    function interupt(err) {
      firstRej(err.message);
      onceSubs.forEach((sub) => sub(err));
      clear();
    }

    function close(reason = "Connection closed unexpectedly. Did sqreamd crash?") {
      if (!alive) return;
      alive = false;
      const err = new Error(reason);
      interupt(err);
      errorSubs.clear();
      send({"closeConnection": "closeConnection"});
      client.destroy();
    }
    
    function firstBuffer(timeout = 3000) {
      if (!alive) return Promise.reject(new Error("Connection not alive"));
      let firstBufferTimeout = null;
      return new Promise((firstRes, firstEnd) => {
        first = (data) => {
          clearTimeout(firstBufferTimeout);
          firstRes(data);
        };
        firstRej = (reason = "Timed out for first buffer.") => {
          clearTimeout(firstBufferTimeout);
          firstEnd(new Error(reason));
        };
        firstBufferTimeout = setTimeout(() => {
          firstRej();
        }, timeout);
      });
    }

    function setMaxRows(rowsNum) {
      maxRows = rowsNum;
    }

    function isAlive() {
      return alive;
    }

    function getServerProtocolVersion() {
      return serverProtocolVersion;
    }

    function getClientProtocolVersion() {
      return handleMessage.clientProtocolVersion;
    }

    /** @type {Buffer[]} */
    const bufferArray = [];
    let bufferLength = 0;
    let isJson = true;
    let colSzsMsg = {};
    let colTypesMsg = {};
    let varchar_encoding = "ascii";

    client.on('data', function handleBuffer(bufferPart) {
      if (first) {
        first(bufferPart);
        first = null;
        return;
      }
      bufferArray.push(bufferPart);
      bufferLength += bufferPart.byteLength;
      const msglen = bufferArray[0].readUInt32LE(2);
      if (bufferLength < msglen + 10) {
        return;
      }
      let buffer;
      try {
        buffer = Buffer.concat(bufferArray);
      } catch (err) {
        interupt(err);
        return;
      }
      serverProtocolVersion = handleMessage.getProtocolVersion(buffer);
      bufferArray.length = 0;
      bufferLength = 0;
      const remaining = buffer.slice(msglen + 10);
      let data;
      try {
        if (isJson) {
          data = JSON.parse(buffer.slice(10, msglen + 10).toString());
          if (data.queryTypeNamed) {
            colTypesMsg = data;
          } else if (data.colSzs) {
            if (data.rows) {
              isJson = false;
              colSzsMsg = data;
            } else {
              data.done = true;
            }
          } else if (data.varcharEncoding) {
            varchar_encoding = data.varcharEncoding.includes('874') ? 'cp874' : data.varcharEncoding || 'ascii';
          }
        } else {
          isJson = true;
          data = { data: handleMessage.parseColsData(buffer, maxRows, varchar_encoding, colTypesMsg, colSzsMsg)}
        }
      } catch (err) {
        isJson = true;
        bufferArray.length = 0;
        bufferLength = 0;
        interupt(err);
        return;
      }
      if (typeof data.data !== 'undefined') {
        const d = subs["data"];
        if (d) {
          d.forEach((callback) => {
            callback(data.data);
          });
        }
      } else if (debug) {
        console.log(`[${time()}]-[sqream]-[recieve]`, data);
      }
      if (data.error) {
        interupt(new Error(data.error));
        return;
      }
      Object.keys(subs).forEach((key) => {
        if (key === "data" || typeof data[key] === 'undefined') return;
        const list = subs[key];
        list.forEach((callback) => {
          callback(data);
        });
      });

      if (remaining.byteLength > 0) {
        handleBuffer(remaining);
      }
    });

    client.on('end', () => {
      close();
    });

    client.on('close', function () {
      if (error) {
        errorSubs.forEach((callback) =>{ 
          callback(error);
        });
        close(error.message);
      }
    });

    client.on('error', function (err) {
      error = err;
    });

    const connectTimer = setTimeout(() => connRej(new Error("Host unreachable or unresponsive.")), 3000);
    client.on('connect', () => {
      clearTimeout(connectTimer);
      connRes({subscribe, once, send, clear, onError, close, interupt, isAlive, firstBuffer, setMaxRows, getClientProtocolVersion, getServerProtocolVersion});
    });

    client.connect({port: port, host: host});

  }).catch((err) => {
    err.connnectionHost = `${host}:${port}`;
    client.destroy(err);
    throw err;
  });
}