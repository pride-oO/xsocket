/**
 * xSocket v2.3.0
 * @licence MIT
 * @author Vadim Pride (pride.mk.ua@gmail.com)
 * @link https://github.com/pride-oO/xsocket
 */
(function (global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? module.exports = factory() :
    typeof define === 'function' && define.amd ? define(factory) :
    (global = typeof globalThis !== 'undefined' ? globalThis : global || self, global.xSocket = factory());
})(this, (function () { 'use strict';

    var xSocket = function xSocket(serverUrl, query, settings) {
      return new xSocket.Client(serverUrl, query, settings);
    };
    xSocket.VERSION = '2.3.0';
    xSocket.window = typeof window === 'object' ? window : false;
    xSocket.WebSocket = typeof WebSocket !== 'undefined' ? WebSocket : false;
    xSocket.process = false;

    var EventEmitter = /*#__PURE__*/function () {
      function EventEmitter() {
        this.__listenerMap = Object.create(null);
        this.__maxListeners = 10;
      }

      /**
       * Append EE listener
       * @param name
       * @param callback
       * @param once
       * @return {EventEmitter}
       */
      var _proto = EventEmitter.prototype;
      _proto.on = function on(name, callback, once) {
        if (typeof name !== 'string' || typeof callback !== 'function') {
          return this;
        }
        var listenerList = this.__listenerMap[name] || (this.__listenerMap[name] = []);
        listenerList.push({
          'callback': callback,
          'once': once === true
        });
        if (this.__maxListeners > 0 && listenerList.length > this.__maxListeners) {
          console.warn('EventEmitter ', name, ', exceeded maxListeners : ', this.__maxListeners + '/' + listenerList.length, '. Use EE.setMaxListeners(n)!');
        }
        return this;
      }

      /**
       * Append once EE listener
       * @param name
       * @param callback
       * @return {EventEmitter}
       */;
      _proto.once = function once(name, callback) {
        return this.on(name, callback, true);
      }

      /**
       * Getter all listener name
       * @returns {string[]}
       */;
      _proto.getListerensName = function getListerensName() {
        return Object.keys(this.__listenerMap);
      }

      /**
       * Clear all listener
       * @returns {{}}
       */;
      _proto.clearAllListerens = function clearAllListerens() {
        this.__listenerMap = Object.create(null);
        return this.__listenerMap;
      }

      /**
       * Getter count EE listener
       * @param name
       * @returns {number}
       */;
      _proto.listenerCount = function listenerCount(name) {
        var listenerList = this.__listenerMap[name];
        return listenerList ? listenerList.length : 0;
      }

      /**
       * Set max listeners
       * @param count
       * @returns {boolean}
       */;
      _proto.setMaxListeners = function setMaxListeners(count) {
        if (typeof count !== 'number') {
          return false;
        }
        this.__maxListeners = parseInt(count);
        return true;
      }

      /**
       * Delete EE listener
       * @param name
       * @param callback
       * @return {EventEmitter}
       */;
      _proto.off = function off(name, callback) {
        var listenerList = this.__listenerMap[name];
        if (!listenerList || typeof callback !== 'function') {
          return this;
        }
        for (var i = listenerList.length - 1; i >= 0; i--) {
          if (listenerList[i].callback === callback) {
            listenerList.splice(i, 1);
          }
        }
        if (!listenerList.length) {
          delete this.__listenerMap[name];
        }
        return this;
      }

      /**
       * Delete all EE listener
       * @param name
       * @return {EventEmitter}
       */;
      _proto.offAll = function offAll(name) {
        delete this.__listenerMap[name];
        return this;
      }

      /**
       * Emit EE listener
       * @param name
       * @param argList
       * @return {EventEmitter}
       */;
      _proto.emit = function emit(name) {
        var listenerList = this.__listenerMap[name];
        if (!listenerList) {
          return this;
        }
        listenerList = listenerList.slice();
        for (var _len = arguments.length, argList = new Array(_len > 1 ? _len - 1 : 0), _key = 1; _key < _len; _key++) {
          argList[_key - 1] = arguments[_key];
        }
        for (var i = 0; i < listenerList.length; i++) {
          var currentList = this.__listenerMap[name];
          var index = currentList ? currentList.indexOf(listenerList[i]) : -1;
          if (index === -1) {
            continue;
          }
          if (listenerList[i].once) {
            currentList.splice(index, 1);
            if (!currentList.length) {
              delete this.__listenerMap[name];
            }
          }
          listenerList[i].callback.apply(this, argList);
        }
        return this;
      };
      return EventEmitter;
    }();

    var MSG_SO_UPDATE = 0;
    var MSG_SD_CREATE = 1;
    var MSG_SD_UPDATE = 2;
    var MSG_SO_TIME = 3;

    /**
     *
     * @returns {Window|*}
     */
    var getWindow = function getWindow() {
      return xSocket.window;
    };

    /**
     *
     * @returns {any}
     */
    var getProcess = function getProcess() {
      return xSocket.process;
    };

    /**
     *
     * @param min
     * @param max
     * @returns {number}
     */
    var getRandInt = function getRandInt(min, max) {
      return Math.floor(min + Math.random() * (max + 1 - min));
    };

    /**
     *
     * @param data
     * @returns {boolean}
     */
    var isBinary = function isBinary(data) {
      return typeof ArrayBuffer === 'function' && (data instanceof ArrayBuffer || ArrayBuffer.isView(data));
    };

    /**
     *
     * @param type
     * @returns {number}
     */
    var getMessageDataIndex = function getMessageDataIndex(type) {
      return type === MSG_SD_CREATE ? 4 : 2;
    };

    /**
     *
     * @param text
     * @returns {Uint8Array}
     */
    var encodeText = function encodeText(text) {
      if (typeof TextEncoder === 'function') {
        return new TextEncoder().encode(text);
      }
      var binary = unescape(encodeURIComponent(text));
      var byteList = new Uint8Array(binary.length);
      for (var i = 0; i < binary.length; i++) {
        byteList[i] = binary.charCodeAt(i);
      }
      return byteList;
    };

    /**
     *
     * @param byteList
     * @returns {string}
     */
    var decodeText = function decodeText(byteList) {
      if (typeof TextDecoder === 'function') {
        return new TextDecoder().decode(byteList);
      }
      var binary = '';
      for (var i = 0; i < byteList.length; i++) {
        binary += String.fromCharCode(byteList[i]);
      }
      return decodeURIComponent(escape(binary));
    };

    /**
     *
     * @param body
     * @returns {string|Uint8Array}
     */
    var encodeMessage = function encodeMessage(body) {
      var dataIndex = getMessageDataIndex(body[0]);
      var data = body[dataIndex];
      if (!isBinary(data)) {
        return JSON.stringify(body);
      }
      body = body.slice();
      body[dataIndex] = null;
      var headerBytes = encodeText(JSON.stringify(body));
      var dataBytes = data instanceof ArrayBuffer ? new Uint8Array(data) : new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
      var frame = new Uint8Array(4 + headerBytes.length + dataBytes.length);
      new DataView(frame.buffer).setUint32(0, headerBytes.length);
      frame.set(headerBytes, 4);
      frame.set(dataBytes, 4 + headerBytes.length);
      return frame;
    };

    /**
     *
     * @param raw
     * @returns {Array|boolean}
     */
    var decodeMessage = function decodeMessage(raw) {
      var body;
      try {
        if (typeof raw === 'string') {
          body = JSON.parse(raw);
        } else {
          var frame = raw instanceof ArrayBuffer ? new Uint8Array(raw) : raw;
          var headerLength = new DataView(frame.buffer, frame.byteOffset, frame.byteLength).getUint32(0);
          body = JSON.parse(decodeText(frame.subarray(4, 4 + headerLength)));
          if (Array.isArray(body)) {
            body[getMessageDataIndex(body[0])] = frame.subarray(4 + headerLength);
          }
        }
      } catch (e) {
        return false;
      }
      return Array.isArray(body) && typeof body[0] === 'number' ? body : false;
    };
    var helpers = {
      getWindow: getWindow,
      getProcess: getProcess,
      getRandInt: getRandInt,
      isBinary: isBinary,
      getMessageDataIndex: getMessageDataIndex,
      encodeText: encodeText,
      decodeText: decodeText,
      encodeMessage: encodeMessage,
      decodeMessage: decodeMessage,
      EventEmitter: EventEmitter
    };

    function _inheritsLoose(t, o) {
      t.prototype = Object.create(o.prototype), t.prototype.constructor = t, _setPrototypeOf(t, o);
    }
    function _setPrototypeOf(t, e) {
      return _setPrototypeOf = Object.setPrototypeOf ? Object.setPrototypeOf.bind() : function (t, e) {
        return t.__proto__ = e, t;
      }, _setPrototypeOf(t, e);
    }

    /**
     * @this {Promise}
     */
    function finallyConstructor(callback) {
      var constructor = this.constructor;
      return this.then(function (value) {
        // @ts-ignore
        return constructor.resolve(callback()).then(function () {
          return value;
        });
      }, function (reason) {
        // @ts-ignore
        return constructor.resolve(callback()).then(function () {
          // @ts-ignore
          return constructor.reject(reason);
        });
      });
    }

    function allSettled(arr) {
      var P = this;
      return new P(function (resolve, reject) {
        if (!(arr && typeof arr.length !== 'undefined')) {
          return reject(new TypeError(typeof arr + ' ' + arr + ' is not iterable(cannot read property Symbol(Symbol.iterator))'));
        }
        var args = Array.prototype.slice.call(arr);
        if (args.length === 0) return resolve([]);
        var remaining = args.length;
        function res(i, val) {
          if (val && (typeof val === 'object' || typeof val === 'function')) {
            var then = val.then;
            if (typeof then === 'function') {
              then.call(val, function (val) {
                res(i, val);
              }, function (e) {
                args[i] = {
                  status: 'rejected',
                  reason: e
                };
                if (--remaining === 0) {
                  resolve(args);
                }
              });
              return;
            }
          }
          args[i] = {
            status: 'fulfilled',
            value: val
          };
          if (--remaining === 0) {
            resolve(args);
          }
        }
        for (var i = 0; i < args.length; i++) {
          res(i, args[i]);
        }
      });
    }

    /**
     * @constructor
     */
    function AggregateError(errors, message) {
      this.name = 'AggregateError', this.errors = errors;
      this.message = message || '';
    }
    AggregateError.prototype = Error.prototype;
    function any(arr) {
      var P = this;
      return new P(function (resolve, reject) {
        if (!(arr && typeof arr.length !== 'undefined')) {
          return reject(new TypeError('Promise.any accepts an array'));
        }
        var args = Array.prototype.slice.call(arr);
        if (args.length === 0) return reject();
        var rejectionReasons = [];
        for (var i = 0; i < args.length; i++) {
          try {
            P.resolve(args[i]).then(resolve).catch(function (error) {
              rejectionReasons.push(error);
              if (rejectionReasons.length === args.length) {
                reject(new AggregateError(rejectionReasons, 'All promises were rejected'));
              }
            });
          } catch (ex) {
            reject(ex);
          }
        }
      });
    }

    // Store setTimeout reference so promise-polyfill will be unaffected by
    // other code modifying setTimeout (like sinon.useFakeTimers())
    var setTimeoutFunc = setTimeout;
    function isArray(x) {
      return Boolean(x && typeof x.length !== 'undefined');
    }
    function noop() {}

    // Polyfill for Function.prototype.bind
    function bind(fn, thisArg) {
      return function () {
        fn.apply(thisArg, arguments);
      };
    }

    /**
     * @constructor
     * @param {Function} fn
     */
    function Promise$2(fn) {
      if (!(this instanceof Promise$2)) throw new TypeError('Promises must be constructed via new');
      if (typeof fn !== 'function') throw new TypeError('not a function');
      /** @type {!number} */
      this._state = 0;
      /** @type {!boolean} */
      this._handled = false;
      /** @type {Promise|undefined} */
      this._value = undefined;
      /** @type {!Array<!Function>} */
      this._deferreds = [];
      doResolve(fn, this);
    }
    function handle(self, deferred) {
      while (self._state === 3) {
        self = self._value;
      }
      if (self._state === 0) {
        self._deferreds.push(deferred);
        return;
      }
      self._handled = true;
      Promise$2._immediateFn(function () {
        var cb = self._state === 1 ? deferred.onFulfilled : deferred.onRejected;
        if (cb === null) {
          (self._state === 1 ? resolve : reject)(deferred.promise, self._value);
          return;
        }
        var ret;
        try {
          ret = cb(self._value);
        } catch (e) {
          reject(deferred.promise, e);
          return;
        }
        resolve(deferred.promise, ret);
      });
    }
    function resolve(self, newValue) {
      try {
        // Promise Resolution Procedure: https://github.com/promises-aplus/promises-spec#the-promise-resolution-procedure
        if (newValue === self) throw new TypeError('A promise cannot be resolved with itself.');
        if (newValue && (typeof newValue === 'object' || typeof newValue === 'function')) {
          var then = newValue.then;
          if (newValue instanceof Promise$2) {
            self._state = 3;
            self._value = newValue;
            finale(self);
            return;
          } else if (typeof then === 'function') {
            doResolve(bind(then, newValue), self);
            return;
          }
        }
        self._state = 1;
        self._value = newValue;
        finale(self);
      } catch (e) {
        reject(self, e);
      }
    }
    function reject(self, newValue) {
      self._state = 2;
      self._value = newValue;
      finale(self);
    }
    function finale(self) {
      if (self._state === 2 && self._deferreds.length === 0) {
        Promise$2._immediateFn(function () {
          if (!self._handled) {
            Promise$2._unhandledRejectionFn(self._value);
          }
        });
      }
      for (var i = 0, len = self._deferreds.length; i < len; i++) {
        handle(self, self._deferreds[i]);
      }
      self._deferreds = null;
    }

    /**
     * @constructor
     */
    function Handler(onFulfilled, onRejected, promise) {
      this.onFulfilled = typeof onFulfilled === 'function' ? onFulfilled : null;
      this.onRejected = typeof onRejected === 'function' ? onRejected : null;
      this.promise = promise;
    }

    /**
     * Take a potentially misbehaving resolver function and make sure
     * onFulfilled and onRejected are only called once.
     *
     * Makes no guarantees about asynchrony.
     */
    function doResolve(fn, self) {
      var done = false;
      try {
        fn(function (value) {
          if (done) return;
          done = true;
          resolve(self, value);
        }, function (reason) {
          if (done) return;
          done = true;
          reject(self, reason);
        });
      } catch (ex) {
        if (done) return;
        done = true;
        reject(self, ex);
      }
    }
    Promise$2.prototype['catch'] = function (onRejected) {
      return this.then(null, onRejected);
    };
    Promise$2.prototype.then = function (onFulfilled, onRejected) {
      // @ts-ignore
      var prom = new this.constructor(noop);
      handle(this, new Handler(onFulfilled, onRejected, prom));
      return prom;
    };
    Promise$2.prototype['finally'] = finallyConstructor;
    Promise$2.all = function (arr) {
      return new Promise$2(function (resolve, reject) {
        if (!isArray(arr)) {
          return reject(new TypeError('Promise.all accepts an array'));
        }
        var args = Array.prototype.slice.call(arr);
        if (args.length === 0) return resolve([]);
        var remaining = args.length;
        function res(i, val) {
          try {
            if (val && (typeof val === 'object' || typeof val === 'function')) {
              var then = val.then;
              if (typeof then === 'function') {
                then.call(val, function (val) {
                  res(i, val);
                }, reject);
                return;
              }
            }
            args[i] = val;
            if (--remaining === 0) {
              resolve(args);
            }
          } catch (ex) {
            reject(ex);
          }
        }
        for (var i = 0; i < args.length; i++) {
          res(i, args[i]);
        }
      });
    };
    Promise$2.any = any;
    Promise$2.allSettled = allSettled;
    Promise$2.resolve = function (value) {
      if (value && typeof value === 'object' && value.constructor === Promise$2) {
        return value;
      }
      return new Promise$2(function (resolve) {
        resolve(value);
      });
    };
    Promise$2.reject = function (value) {
      return new Promise$2(function (resolve, reject) {
        reject(value);
      });
    };
    Promise$2.race = function (arr) {
      return new Promise$2(function (resolve, reject) {
        if (!isArray(arr)) {
          return reject(new TypeError('Promise.race accepts an array'));
        }
        for (var i = 0, len = arr.length; i < len; i++) {
          Promise$2.resolve(arr[i]).then(resolve, reject);
        }
      });
    };

    // Use polyfill for setImmediate for performance gains
    Promise$2._immediateFn =
    // @ts-ignore
    typeof setImmediate === 'function' && function (fn) {
      // @ts-ignore
      setImmediate(fn);
    } || function (fn) {
      setTimeoutFunc(fn, 0);
    };
    Promise$2._unhandledRejectionFn = function _unhandledRejectionFn(err) {
      if (typeof console !== 'undefined' && console) {
        console.warn('Possible Unhandled Promise Rejection:', err); // eslint-disable-line no-console
      }
    };

    var Promise$1 = typeof Promise === 'function' && typeof Promise.prototype.then === 'function' ? Promise : Promise$2;

    var Data = /*#__PURE__*/function (_EventEmitter) {
      /**
       *
       * @param __isOutput
       * @param __object
       * @param __socketObject
       */
      function Data(__isOutput, __object, __socketObject) {
        var _this;
        _this = _EventEmitter.call(this) || this;
        _this.setMaxListeners(0);
        __object = typeof __object === 'object' && __object ? __object : {};
        _this.__isOutput = !!__isOutput;
        _this.__socketObject = __socketObject || false;
        _this.__ID = typeof __object.ID === 'string' ? __object.ID : '';
        _this.__type = typeof __object.type === 'string' ? __object.type : '';
        _this.__data = typeof __object.data === 'object' ? __object.data : {};
        _this.__ttl = typeof __object.ttl === 'number' && __object.ttl % 1 === 0 && __object.ttl >= 0 ? __object.ttl : Data.DEFAULT_TTL;
        var now = _this.__socketObject ? _this.__socketObject.getTime() : Date.now();
        _this.__time = typeof __object.time === 'number' && __object.time > 0 ? __object.time : now;
        _this.__resData = {};
        _this.__error = false;
        _this.__status = Data.STATUS_NEW;
        _this.__destroy = false;
        _this.__ttlTimeout = setTimeout(function () {
          _this.destroy('ttl', true);
        }, Math.max(0, Math.min(_this.__ttl, _this.__ttl - (now - _this.__time))));
        return _this;
      }

      /**
       *
       * @returns {string}
       */
      _inheritsLoose(Data, _EventEmitter);
      var _proto = Data.prototype;
      _proto.getID = function getID() {
        return String(this.__ID);
      }

      /**
       *
       * @returns {string}
       */;
      _proto.getType = function getType() {
        return String(this.__type);
      }

      /**
       *
       * @returns {number}
       */;
      _proto.getTTL = function getTTL() {
        return this.__ttl;
      }

      /**
       *
       * @returns {number}
       */;
      _proto.getTime = function getTime() {
        return this.__time;
      }

      /**
       *
       * @returns {{}}
       */;
      _proto.getData = function getData() {
        return this.__data || {};
      }

      /**
       *
       * @param key
       * @param def
       * @returns {*}
       */;
      _proto.getDataVal = function getDataVal(key, def) {
        return this.getData()[key] || def;
      }

      /**
       *
       * @returns {{}}
       */;
      _proto.getResData = function getResData() {
        return this.__resData || {};
      }

      /**
       *
       * @param key
       * @param def
       * @returns {*}
       */;
      _proto.getResDataVal = function getResDataVal(key, def) {
        return this.getResData()[key] || def;
      }

      /**
       *
       * @returns {number}
       */;
      _proto.getStatus = function getStatus() {
        return this.__status;
      }

      /**
       *
       * @returns {boolean|string}
       */;
      _proto.getDestroy = function getDestroy() {
        return this.__destroy || false;
      }

      /**
       *
       * @returns {SocketObject|boolean}
       */;
      _proto.getSocketObject = function getSocketObject() {
        return this.__socketObject;
      }

      /**
       *
       * @returns {string|boolean}
       */;
      _proto.getError = function getError() {
        return this.__error || false;
      }

      /**
       *
       * @returns {boolean}
       */;
      _proto.isNew = function isNew() {
        return this.__status === Data.STATUS_NEW;
      }

      /**
       *
       * @returns {boolean}
       */;
      _proto.isSend = function isSend() {
        return this.__status === Data.STATUS_SEND;
      }

      /**
       *
       * @returns {boolean}
       */;
      _proto.isResponse = function isResponse() {
        return this.__status === Data.STATUS_RESPONSE;
      }

      /**
       *
       * @param data
       * @param error
       * @returns {Promise<Data>}
       */;
      _proto.response = function response(data, error) {
        var _this2 = this;
        var promise = new Promise$1(function (resolve, reject) {
          if (_this2.__isOutput) {
            return reject(new Error('Output data'));
          }
          if (_this2.__destroy || _this2.isResponse() || !_this2.__ttl) {
            return reject(new Error('destroy'));
          }
          _this2.setResData(data);
          _this2.setError(error);
          _this2.once('response', function () {
            resolve(_this2);
          });
          _this2.once('destroy', function (msg) {
            reject(new Error(msg));
          });
          _this2.__socketObject.getWsReady().then(function (ws) {
            if (ws.sendObject([MSG_SD_UPDATE, Number(_this2.__ID), _this2.getResData(), _this2.getError()])) {
              _this2.setStatus(Data.STATUS_RESPONSE);
            } else {
              _this2.destroy('Send failed');
            }
          }).catch(function (e) {
            _this2.destroy(e.message || 'Send failed');
          });
        });
        promise.catch(function () {});
        return promise;
      }

      /**
       *
       * @param status
       * @returns {boolean}
       */;
      _proto.setStatus = function setStatus(status) {
        if (status !== Data.STATUS_SEND && status !== Data.STATUS_RESPONSE || this.__status === Data.STATUS_RESPONSE || this.__status === status) {
          return false;
        }
        this.__status = status;
        this.emit('setStatus', status);
        if (status === Data.STATUS_RESPONSE) {
          this.emit('response', this.getResData(), this.getError());
          this.destroy('response');
        }
        return true;
      }

      /**
       *
       * @param errorMsg
       * @returns {boolean}
       */;
      _proto.setError = function setError(errorMsg) {
        if (typeof errorMsg !== 'string') {
          return false;
        }
        this.__error = errorMsg;
        return true;
      }

      /**
       *
       * @param data
       * @returns {boolean}
       */;
      _proto.setResData = function setResData(data) {
        if (typeof data !== 'object') {
          return false;
        }
        this.__resData = data;
        return true;
      }

      /**
       *
       * @param msg
       * @param isSetError
       * @returns {boolean}
       */;
      _proto.destroy = function destroy(msg, isSetError) {
        if (this.__destroy) {
          return false;
        }
        this.__destroy = typeof msg === 'string' && msg.length ? msg : 'destroy';
        clearTimeout(this.__ttlTimeout);
        if (isSetError && !this.__error) {
          this.setError(this.__destroy);
        }
        if (this.__status !== Data.STATUS_RESPONSE) {
          this.setStatus(Data.STATUS_RESPONSE);
        }
        this.emit('destroy', this.__destroy);
        this.clearAllListerens();
        return true;
      };
      return Data;
    }(EventEmitter);
    Data.STATUS_NEW = 0;
    Data.STATUS_SEND = 1;
    Data.STATUS_RESPONSE = 2;
    Data.DEFAULT_TTL = 3000;

    var SocketObject = /*#__PURE__*/function (_EventEmitter) {
      /**
       *
       * @param __ServerConfigure
       * @param __req
       */
      function SocketObject(__ServerConfigure, __req) {
        var _this;
        _this = _EventEmitter.call(this) || this;
        _this.setMaxListeners(0);
        _this.__serverConfigure = __ServerConfigure;
        _this.__req = __req;
        _this.__ws = undefined;
        _this.__ID = '';
        _this.__sign = '';
        _this.__destroy = undefined;
        _this.__isNewConnection = true;
        _this.__socketDataMap = {};
        _this.__socketDataInc = 0;
        _this.__query = {};
        _this.__timeOffset = 0;
        _this.on('destroy', _this.__onDestroy);
        _this.on('ws|disconnect', _this.__onWsDisconnect);
        _this.on('ws|connect', _this.__onWsConnect);
        _this.on('ws|message', _this.__onWsMessage);
        return _this;
      }

      /**
       *
       * @returns {string}
       */
      _inheritsLoose(SocketObject, _EventEmitter);
      var _proto = SocketObject.prototype;
      _proto.getID = function getID() {
        return this.__ID || '';
      }

      /**
       *
       * @returns {string}
       */;
      _proto.getSign = function getSign() {
        return this.__sign || '';
      }

      /**
       *
       * @returns {number}
       */;
      _proto.getTime = function getTime() {
        return Date.now() + this.__timeOffset;
      }

      /**
       *
       * @returns {number}
       */;
      _proto.getTimeOffset = function getTimeOffset() {
        return this.__timeOffset;
      }

      /**
       *
       * @param timeOffset
       * @returns {boolean}
       */;
      _proto.setTimeOffset = function setTimeOffset(timeOffset) {
        if (typeof timeOffset !== 'number' || timeOffset !== timeOffset) {
          return false;
        }
        this.__timeOffset = Math.round(timeOffset);
        return true;
      }

      /**
       *
       * @returns {*|{}}
       */;
      _proto.getReq = function getReq() {
        return this.__req || {};
      }

      /**
       *
       * @returns {*}
       */;
      _proto.getQuery = function getQuery() {
        return typeof this.__query === 'object' ? this.__query : {};
      }

      /**
       *
       * @param key
       * @param def
       * @returns {*}
       */;
      _proto.getQueryVal = function getQueryVal(key, def) {
        return this.getQuery()[key] || def;
      }

      /**
       *
       * @returns {*|{}}
       */;
      _proto.getReqHeaders = function getReqHeaders() {
        return this.getReq()['headers'] || {};
      }

      /**
       *
       * @returns {*|string}
       */;
      _proto.getOrigin = function getOrigin() {
        return this.getReqHeaders()['origin'] || 'unknown';
      }

      /**
       *
       * @returns {string}
       */;
      _proto.getIP = function getIP() {
        return String((this.getReq()['connection'] || {})['remoteAddress'] || 'unknown');
      }

      /**
       *
       * @returns {{}}
       */;
      _proto.getSocketDataList = function getSocketDataList() {
        return this.__socketDataMap;
      }

      /**
       *
       * @param ID
       * @returns {Data|boolean}
       */;
      _proto.getSocketData = function getSocketData(ID) {
        return this.__socketDataMap[ID] || false;
      }

      /**
       *
       * @returns {boolean}
       */;
      _proto.isConnection = function isConnection() {
        return typeof this.__ws === 'object' ? this.__ws.isConnection() : false;
      }

      /**
       *
       * @returns {any|boolean}
       */;
      _proto.getConnection = function getConnection() {
        return this.isConnection() ? this.__ws : false;
      }

      /**
       *
       * @returns {string|boolean}
       */;
      _proto.getDestroy = function getDestroy() {
        return this.__destroy || false;
      }

      /**
       *
       * @returns {boolean}
       */;
      _proto.isDestroy = function isDestroy() {
        return !!this.getDestroy();
      }

      /**
       *
       * @returns {boolean}
       */;
      _proto.isServer = function isServer() {
        return !!this.__serverConfigure;
      }

      /**
       *
       * @returns {Promise}
       */;
      _proto.getWsReady = function getWsReady() {
        var _this2 = this;
        return new Promise$1(function (resolve, reject) {
          if (_this2.getConnection()) {
            return resolve(_this2.getConnection());
          }
          var destroyHandler = function destroyHandler() {
            _this2.off('connect', connectHandler);
            reject(new Error('destroy'));
          };
          var connectHandler = function connectHandler() {
            _this2.off('destroy', destroyHandler);
            if (_this2.getConnection()) {
              resolve(_this2.getConnection());
            } else {
              reject(new Error('Connection failed'));
            }
          };
          _this2.once('connect', connectHandler);
          _this2.once('destroy', destroyHandler);
        });
      }

      /**
       *
       * @param type
       * @param data
       * @param ttl
       * @returns {Promise<Data>}
       */;
      _proto.send = function send(type, data, ttl) {
        var _this3 = this;
        var socketData = new Data(true, {
          'ID': String(++this.__socketDataInc),
          'type': type,
          'data': data,
          'ttl': ttl,
          'time': this.getTime()
        }, this);
        if (socketData.getTTL()) {
          this.__socketDataMap[socketData.getID()] = socketData;
          socketData.once('destroy', function () {
            delete _this3.__socketDataMap[socketData.getID()];
          });
        }
        this.getWsReady().then(function (ws) {
          if (!ws.sendObject([MSG_SD_CREATE, Number(socketData.getID()), socketData.getType(), socketData.getTTL(), socketData.getData(), socketData.getTime()])) {
            return socketData.destroy('Send failed');
          }
          socketData.setStatus(socketData.getTTL() ? Data.STATUS_SEND : Data.STATUS_RESPONSE);
        }).catch(function (e) {
          socketData.destroy(e.message || 'Send failed');
        });
        return Promise$1.resolve(socketData);
      }

      /**
       *
       * @param type
       * @param data
       * @param ttl
       * @returns {Promise<Data>}
       */;
      _proto.sendReadyResponse = function sendReadyResponse(type, data, ttl) {
        return this.send(type, data, ttl).then(function (socketData) {
          return new Promise$1(function (resolve, reject) {
            var responseHandler = function responseHandler() {
              if (socketData.getError()) {
                return reject(new Error(socketData.getError()));
              }
              resolve(socketData);
            };
            if (socketData.isResponse() || socketData.getDestroy()) {
              return responseHandler();
            }
            socketData.once('response', responseHandler);
          });
        });
      }

      /**
       *
       * @param type
       * @param data
       * @param ttl
       * @returns {Promise<Data>}
       */;
      _proto.sendReady = function sendReady(type, data, ttl) {
        return this.sendReadyResponse(type, data, ttl);
      }

      /**
       *
       * @param msg
       * @returns {boolean}
       */;
      _proto.destroy = function destroy(msg) {
        if (this.isDestroy()) {
          return false;
        }
        this.__destroy = String(msg || 'destroy');
        this.disconnect(this.__destroy);
        this.emit('destroy', this, this.__destroy);
        this.clearAllListerens();
        return true;
      }

      /**
       *
       * @param msg
       * @returns {boolean}
       */;
      _proto.disconnect = function disconnect(msg) {
        var ws = this.getConnection();
        if (!ws) {
          return false;
        }
        return ws.closeConnection(msg);
      }

      /**
       *
       * @param ws
       * @returns {boolean}
       */;
      _proto.update = function update(ws) {
        if (typeof ws !== 'object' || typeof ws.isConnection !== 'function' || typeof ws.query !== 'object') {
          return false;
        }
        if (!ws.isConnection() || this.__destroy) {
          return false;
        }
        if (this.__ID !== ws.query['xSOId'] || this.__sign !== ws.query['xSOSign']) {
          this.__ID = ws.query['xSOId'];
          this.__sign = ws.query['xSOSign'];
          this.__query = Object(ws['query'] || {});
          delete this.__query['xSOId'];
          delete this.__query['xSOSign'];
          delete this.__query['xV'];
          this.emit('update', this);
        }
        if (this.isServer()) {
          ws.sendObject([MSG_SO_UPDATE, this.getID(), this.getSign(), typeof this.__serverConfigure === 'object' ? this.__serverConfigure : {}, xSocket.VERSION, Date.now()]);
        } else {
          this.setTimeOffset(ws.timeOffset);
          ws.sendObject([MSG_SO_TIME, Date.now()]);
        }
        this.__ws = ws;
        ws.SocketObject = this;
        this.emit('ws|connect', ws);
        return true;
      };
      _proto.__onDestroy = function __onDestroy() {
        for (var ID in this.__socketDataMap) {
          this.__socketDataMap[ID].destroy('SocketObject|destroy');
        }
      };
      _proto.__onWsDisconnect = function __onWsDisconnect(msg) {
        msg = typeof msg === 'string' ? msg : 'unknown';
        this.emit('disconnect', this, msg);
        if (msg === 'end') {
          this.destroy('end');
        }
      };
      _proto.__onWsConnect = function __onWsConnect() {
        this.emit('connect', this);
        if (!this.__isNewConnection) {
          this.emit('reconnect', this);
        } else {
          this.__isNewConnection = false;
        }
      };
      _proto.__onWsMessage = function __onWsMessage(body) {
        if (body[0] === MSG_SO_TIME) {
          return this.__onWsTime(body);
        }
        var ID = body[1];
        if (typeof ID !== 'number' || ID % 1 !== 0 || ID < 1) {
          return;
        }
        var socketData;
        if (body[0] === MSG_SD_CREATE) {
          if (typeof body[2] !== 'string' || !body[2].length || typeof body[4] !== 'object') {
            return;
          }
          socketData = new Data(false, {
            'ID': String(ID),
            'type': body[2],
            'ttl': body[3],
            'data': body[4],
            'time': body[5]
          }, this);
          this.emit('data', socketData);
          this.emit('data|' + socketData.getType(), socketData);
          if (!socketData.getTTL()) {
            socketData.setStatus(Data.STATUS_RESPONSE);
          }
        } else if (body[0] === MSG_SD_UPDATE) {
          socketData = this.getSocketData(String(ID));
          if (socketData) {
            socketData.setResData(body[2]);
            socketData.setError(body[3] || false);
            socketData.setStatus(Data.STATUS_RESPONSE);
          }
        }
      };
      _proto.__onWsTime = function __onWsTime(body) {
        var ws = this.getConnection();
        if (!ws || typeof body[1] !== 'number') {
          return;
        }
        if (this.isServer()) {
          ws.sendObject([MSG_SO_TIME, body[1], Date.now()]);
        } else if (typeof body[2] === 'number') {
          var now = Date.now();
          this.setTimeOffset(body[2] + (now - body[1]) / 2 - now);
        }
      };
      return SocketObject;
    }(EventEmitter);
    SocketObject.MSG_SO_UPDATE = MSG_SO_UPDATE;
    SocketObject.MSG_SD_CREATE = MSG_SD_CREATE;
    SocketObject.MSG_SD_UPDATE = MSG_SD_UPDATE;
    SocketObject.MSG_SO_TIME = MSG_SO_TIME;

    var Client = /*#__PURE__*/function (_EventEmitter) {
      /**
       *
       * @param serverUrl
       * @param query
       * @param settings
       */
      function Client(serverUrl, query, settings) {
        var _this;
        _this = _EventEmitter.call(this) || this;
        _this.setMaxListeners(0);
        _this.__urlList = [];
        _this.__ws = undefined;
        _this.__socketObject = false;
        _this.__isVersionWarned = false;
        _this.__settings = typeof settings === 'object' && settings ? settings : {};
        _this.__query = typeof query === 'object' && query ? query : {};
        if (typeof serverUrl === 'object' && serverUrl) {
          for (var i in serverUrl) {
            _this.__urlList.push(serverUrl[i]);
          }
        } else if (typeof serverUrl === 'string') {
          _this.__urlList.push(serverUrl);
        }
        if (!_this.__urlList.length) {
          throw new Error('Invalid urlList');
        }
        var reconnect = function reconnect() {
          setTimeout(function () {
            _this.getWs().catch(function () {});
          }, 1000);
        };
        _this.on('ws|disconnect', reconnect);
        _this.on('ws|error', reconnect);
        var closeConnection = function closeConnection() {
          try {
            _this.__ws.closeConnection('end');
          } catch (e) {}
        };
        if (getWindow()) {
          getWindow().addEventListener('pagehide', closeConnection);
          getWindow().addEventListener('unload', closeConnection);
        } else if (getProcess()) {
          getProcess().on('exit', closeConnection);
        }
        _this.on('connect', function (socketObject) {
          var destroyTimeout = false;
          var setDestroyTimeout = function setDestroyTimeout(action, ttl) {
            if (destroyTimeout) {
              clearTimeout(destroyTimeout);
              destroyTimeout = false;
            }
            if (action) {
              ttl = typeof ttl === 'number' && !isNaN(ttl) && ttl > 0 ? ttl : 0;
              destroyTimeout = setTimeout(function () {
                socketObject.destroy('socketObjectTimeout');
                destroyTimeout = false;
              }, ttl);
            }
          };
          socketObject.on('reconnect', function () {
            setDestroyTimeout(false);
          });
          socketObject.on('disconnect', function () {
            var socketObjectTimeout = parseInt(Object(socketObject.getQueryVal('configure', {}))['socketObjectTimeout'] || 0);
            if (typeof socketObjectTimeout === 'number') {
              setDestroyTimeout(true, socketObjectTimeout);
            }
          });
          socketObject.once('destroy', function () {
            setDestroyTimeout(false);
          });
        });
        _this.getWs().catch(function () {});
        return _this;
      }

      /**
       *
       * @returns {{}}
       */
      _inheritsLoose(Client, _EventEmitter);
      var _proto = Client.prototype;
      _proto.getSettings = function getSettings() {
        return this.__settings || {};
      }

      /**
       *
       * @param key
       * @param def
       * @returns {*}
       */;
      _proto.getSettingsVal = function getSettingsVal(key, def) {
        return this.getSettings()[key] || def;
      }

      /**
       *
       * @returns {SocketObject|boolean}
       */;
      _proto.getSocketObject = function getSocketObject() {
        return this.__socketObject;
      }

      /**
       *
       * @returns {Promise<SocketObject>}
       */;
      _proto.getSocketObjectReady = function getSocketObjectReady() {
        var _this2 = this;
        return new Promise$1(function (resolve) {
          if (_this2.getSocketObject()) {
            return resolve(_this2.getSocketObject());
          }
          _this2.once('connect', function (socketObject) {
            resolve(socketObject);
          });
        });
      }

      /**
       *
       * @returns {Promise<{}>}
       */;
      _proto.getWs = function getWs() {
        var _this3 = this;
        return new Promise$1(function (resolve) {
          if (typeof _this3.__ws === 'object') {
            return resolve(_this3.__ws);
          }
          _this3.__ws = null;
          var url = _this3.__urlList[getRandInt(0, _this3.__urlList.length - 1)];
          url += url.indexOf('?') === -1 ? '?' : '&';
          var query = _this3.__query;
          query['xV'] = xSocket.VERSION;
          if (_this3.__socketObject && _this3.__socketObject.getID().length && _this3.__socketObject.getSign().length) {
            query['xSOId'] = String(_this3.__socketObject.getID());
            query['xSOSign'] = String(_this3.__socketObject.getSign());
          }
          for (var i in query) {
            try {
              url += encodeURIComponent(String(i)) + '=' + encodeURIComponent(String(query[i])) + '&';
            } catch (e) {}
          }
          var ws = new xSocket.WebSocket(url);
          ws.binaryType = 'arraybuffer';
          ws.isConnection = function () {
            return ws.readyState === 1;
          };
          ws.closeConnection = function (msg) {
            if (ws.isConnection()) {
              try {
                ws.close(1000, String(msg || 'close'));
                return true;
              } catch (e) {
                console.error(e);
              }
            }
            return false;
          };
          ws.sendMessage = function (msg) {
            try {
              if (ws.isConnection()) {
                ws.send(String(msg));
                return true;
              }
            } catch (e) {}
            return false;
          };
          ws.sendObject = function (object) {
            try {
              if (ws.isConnection()) {
                ws.send(encodeMessage(object));
                return true;
              }
            } catch (e) {}
            return false;
          };
          var closeMsg;
          var msgInc = 0;
          var pongInterval = false;
          ws.onopen = function () {
            _this3.__ws = ws;
            _this3.emit('ws|connect', ws);
            pongInterval = setInterval(function () {
              if (!msgInc) {
                ws.closeConnection('pingTimeout');
              }
              msgInc = 0;
            }, 21000);
            resolve(ws);
          };
          ws.onmessage = function (e) {
            msgInc++;
            if (e.data === 'ping') {
              return ws.sendMessage('pong');
            }
            try {
              var body = decodeMessage(e.data);
              if (!body) {
                return;
              }
              if (body[0] === MSG_SO_UPDATE) {
                _this3.__onSocketObjectUpdate(ws, body);
              } else if (_this3.__socketObject) {
                _this3.__socketObject.emit('ws|message', body);
              }
            } catch (e) {}
          };
          ws.onclose = function (e) {
            if (pongInterval) {
              clearInterval(pongInterval);
            }
            _this3.__ws = undefined;
            closeMsg = closeMsg || e.reason || 'close';
            _this3.emit('ws|disconnect', closeMsg);
            if (_this3.__socketObject) {
              _this3.__socketObject.emit('ws|disconnect', closeMsg);
            }
          };
          ws.onerror = function (e) {
            closeMsg = e.message;
            _this3.emit('ws|error', e.message, ws);
          };
        });
      }

      /**
       *
       * @param type
       * @param data
       * @param ttl
       * @returns {Promise<Data>}
       */;
      _proto.send = function send(type, data, ttl) {
        return this.getSocketObjectReady().then(function (socketObject) {
          return socketObject.send(type, data, ttl);
        });
      }

      /**
       *
       * @param type
       * @param data
       * @param ttl
       * @returns {Promise<Data>}
       */;
      _proto.sendReady = function sendReady(type, data, ttl) {
        return this.getSocketObjectReady().then(function (socketObject) {
          return socketObject.sendReady(type, data, ttl);
        });
      }

      /**
       *
       * @param type
       * @param data
       * @param ttl
       * @returns {Promise<Data>}
       */;
      _proto.sendReadyResponse = function sendReadyResponse(type, data, ttl) {
        return this.sendReady(type, data, ttl);
      }

      /**
       *
       * @param msg
       * @returns {boolean}
       */;
      _proto.disconnect = function disconnect(msg) {
        var socketObject = this.getSocketObject();
        if (!socketObject) {
          return false;
        }
        return socketObject.disconnect(msg);
      };
      _proto.__onSocketObjectUpdate = function __onSocketObjectUpdate(ws, body) {
        var _this4 = this;
        if (body[4] !== xSocket.VERSION && !this.__isVersionWarned) {
          this.__isVersionWarned = true;
          console.warn('[warn|xSocket]', 'Version mismatch', 'client ' + xSocket.VERSION + ', server ' + (body[4] || 'unknown'));
        }
        ws.timeOffset = typeof body[5] === 'number' ? body[5] - Date.now() : 0;
        ws.query = {
          'xSOId': String(body[1]),
          'xSOSign': String(body[2]),
          'configure': typeof body[3] === 'object' ? body[3] : {}
        };
        if (this.__socketObject) {
          if (ws.query['xSOId'] === this.__socketObject.getID() && ws.query['xSOSign'] === this.__socketObject.getSign() && this.__socketObject.update(ws)) {
            return;
          }
          this.__socketObject.destroy('update');
        }
        var socketObject = this.__socketObject = new SocketObject();
        socketObject.once('connect', function () {
          _this4.emit('connect', socketObject);
        });
        socketObject.on('destroy', function (socketObject, msg) {
          _this4.__socketObject = false;
          _this4.emit('destroy', socketObject, msg);
        });
        socketObject.on('disconnect', function (socketObject, msg) {
          _this4.emit('disconnect', socketObject, msg);
        });
        socketObject.on('reconnect', function () {
          _this4.emit('reconnect', socketObject);
        });
        socketObject.on('data', function (socketData) {
          _this4.emit('data', socketData);
        });
        if (!socketObject.update(ws)) {
          ws.closeConnection('update');
        }
      };
      return Client;
    }(EventEmitter);

    xSocket.helpers = helpers;
    xSocket.EventEmitter = EventEmitter;
    xSocket.Data = Data;
    xSocket.xSocketObject = SocketObject;
    xSocket.Client = Client;
    xSocket.VERSION;

    return xSocket;

}));
