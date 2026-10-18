/**
 *
 * @param __isOutput
 * @param __object
 * @param __socketObject
 * @constructor
 */
xSocket.Data = function xSocketData(__isOutput, __object, __socketObject){
    xSocket.helpers.EventEmitter.call(this);
    this.setMaxListeners(0);
    __object = typeof __object === 'object' && __object ? __object : {};

    var $this = this;
    this.__isOutput     = !!__isOutput;
    this.__socketObject = __socketObject || false;
    this.__ID           = typeof __object.ID === 'string' ? __object.ID : '';
    this.__type         = typeof __object.type === 'string' ? __object.type : '';
    this.__data         = typeof __object.data === 'object' ? __object.data : {};
    this.__ttl          = typeof __object.ttl === 'number' && __object.ttl % 1 === 0 && __object.ttl >= 0 ? __object.ttl : xSocket.Data.DEFAULT_TTL;
    var now             = this.__socketObject ? this.__socketObject.getTime() : Date.now();
    this.__time         = typeof __object.time === 'number' && __object.time > 0 ? __object.time : now;
    this.__resData      = {};
    this.__error        = false;
    this.__status       = xSocket.Data.STATUS_NEW;
    this.__destroy      = false;
    this.__ttlTimeout   = setTimeout(function (){
        $this.destroy('ttl', true);
    }, Math.max(0, Math.min(this.__ttl, this.__ttl - (now - this.__time))));
};
xSocket.Data.prototype = Object.create(xSocket.helpers.EventEmitter.prototype);
xSocket.Data.prototype.constructor = xSocket.Data;
xSocket.Data.STATUS_NEW      = 0;
xSocket.Data.STATUS_SEND     = 1;
xSocket.Data.STATUS_RESPONSE = 2;
xSocket.Data.DEFAULT_TTL     = 3000;

/**
 *
 * @returns {string}
 */
xSocket.Data.prototype.getID = function (){
    return String(this.__ID);
};

/**
 *
 * @returns {string}
 */
xSocket.Data.prototype.getType = function (){
    return String(this.__type);
};

/**
 *
 * @returns {number}
 */
xSocket.Data.prototype.getTTL = function (){
    return this.__ttl;
};

/**
 *
 * @returns {number}
 */
xSocket.Data.prototype.getTime = function (){
    return this.__time;
};

/**
 *
 * @returns {{}}
 */
xSocket.Data.prototype.getData = function (){
    return this.__data || {};
};

/**
 *
 * @param key
 * @param def
 * @returns {*}
 */
xSocket.Data.prototype.getDataVal = function (key, def){
    return this.getData()[key] || def;
};

/**
 *
 * @returns {{}}
 */
xSocket.Data.prototype.getResData = function (){
    return this.__resData || {};
};

/**
 *
 * @param key
 * @param def
 * @returns {*}
 */
xSocket.Data.prototype.getResDataVal = function (key, def){
    return this.getResData()[key] || def;
};

/**
 *
 * @returns {number}
 */
xSocket.Data.prototype.getStatus = function (){
    return this.__status;
};

/**
 *
 * @returns {boolean|string}
 */
xSocket.Data.prototype.getDestroy = function (){
    return this.__destroy || false;
};

/**
 *
 * @returns {xSocket.xSocketObject|boolean}
 */
xSocket.Data.prototype.getSocketObject = function (){
    return this.__socketObject;
};

/**
 *
 * @returns {string|boolean}
 */
xSocket.Data.prototype.getError = function (){
    return this.__error || false;
};

/**
 *
 * @returns {boolean}
 */
xSocket.Data.prototype.isNew = function (){
    return this.__status === xSocket.Data.STATUS_NEW;
};

/**
 *
 * @returns {boolean}
 */
xSocket.Data.prototype.isSend = function (){
    return this.__status === xSocket.Data.STATUS_SEND;
};

/**
 *
 * @returns {boolean}
 */
xSocket.Data.prototype.isResponse = function (){
    return this.__status === xSocket.Data.STATUS_RESPONSE;
};

/**
 *
 * @param data
 * @param error
 * @returns {Promise<xSocket.Data>}
 */
xSocket.Data.prototype.response = function (data, error){
    var $this = this;
    var promise = new Promise(function (resolve, reject){
        if($this.__isOutput){
            return reject(new Error('Output data'));
        }
        if($this.__destroy || $this.isResponse() || !$this.__ttl){
            return reject(new Error('destroy'));
        }
        $this.setResData(data);
        $this.setError(error);
        $this.once('response', function (){
            resolve($this);
        });
        $this.once('destroy', function (msg){
            reject(new Error(msg));
        });
        $this.__socketObject.getWsReady().then(function (ws){
            if(ws.sendObject([
                xSocket.xSocketObject.MSG_SD_UPDATE,
                Number($this.__ID),
                $this.getResData(),
                $this.getError()
            ])){
                $this.setStatus(xSocket.Data.STATUS_RESPONSE);
            }else{
                $this.destroy('Send failed');
            }
        }).catch(function (e){
            $this.destroy(e.message || 'Send failed');
        });
    });
    promise.catch(function (){});
    return promise;
};

/**
 *
 * @param status
 * @returns {boolean}
 */
xSocket.Data.prototype.setStatus = function (status){
    if(
        (status !== xSocket.Data.STATUS_SEND && status !== xSocket.Data.STATUS_RESPONSE) ||
        this.__status === xSocket.Data.STATUS_RESPONSE ||
        this.__status === status
    ){
        return false;
    }
    this.__status = status;
    this.emit('setStatus', status);
    if(status === xSocket.Data.STATUS_RESPONSE){
        this.emit('response', this.getResData(), this.getError());
        this.destroy('response');
    }
    return true;
};

/**
 *
 * @param errorMsg
 * @returns {boolean}
 */
xSocket.Data.prototype.setError = function (errorMsg){
    if(typeof errorMsg !== 'string'){
        return false;
    }
    this.__error = errorMsg;
    return true;
};

/**
 *
 * @param data
 * @returns {boolean}
 */
xSocket.Data.prototype.setResData = function (data){
    if(typeof data !== 'object'){
        return false;
    }
    this.__resData = data;
    return true;
};

/**
 *
 * @param msg
 * @param isSetError
 * @returns {boolean}
 */
xSocket.Data.prototype.destroy = function (msg, isSetError){
    if(this.__destroy){
        return false;
    }
    this.__destroy = typeof msg === 'string' && msg.length ? msg : 'destroy';
    clearTimeout(this.__ttlTimeout);
    if(isSetError && !this.__error){
        this.setError(this.__destroy);
    }
    if(this.__status !== xSocket.Data.STATUS_RESPONSE){
        this.setStatus(xSocket.Data.STATUS_RESPONSE);
    }
    this.emit('destroy', this.__destroy);
    this.clearAllListerens();
    return true;
};
