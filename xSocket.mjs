/**
 * xSocket v2.3.0
 * @licence MIT
 * @author Vadim Pride (pride.mk.ua@gmail.com)
 * @link https://github.com/pride-oO/xsocket
 */
const xSocket = function xSocket(serverUrl, query, settings){
    return new xSocket.Client(serverUrl, query, settings);
};
xSocket.VERSION   = '2.3.0';
xSocket.window    = typeof window === 'object' ? window : false;
xSocket.WebSocket = typeof WebSocket !== 'undefined' ? WebSocket : false;
xSocket.process   = false;

class EventEmitter {

    constructor(){
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
    on(name, callback, once){
        if(typeof name !== 'string' || typeof callback !== 'function'){
            return this;
        }
        const listenerList = this.__listenerMap[name] || (this.__listenerMap[name] = []);
        listenerList.push({'callback' : callback, 'once' : once === true});
        if(this.__maxListeners > 0 && listenerList.length > this.__maxListeners){
            console.warn('EventEmitter ', name, ', exceeded maxListeners : ', this.__maxListeners + '/' + listenerList.length, '. Use EE.setMaxListeners(n)!');
        }
        return this;
    }

    /**
     * Append once EE listener
     * @param name
     * @param callback
     * @return {EventEmitter}
     */
    once(name, callback){
        return this.on(name, callback, true);
    }

    /**
     * Getter all listener name
     * @returns {string[]}
     */
    getListerensName(){
        return Object.keys(this.__listenerMap);
    }

    /**
     * Clear all listener
     * @returns {{}}
     */
    clearAllListerens(){
        this.__listenerMap = Object.create(null);
        return this.__listenerMap;
    }

    /**
     * Getter count EE listener
     * @param name
     * @returns {number}
     */
    listenerCount(name){
        const listenerList = this.__listenerMap[name];
        return listenerList ? listenerList.length : 0;
    }

    /**
     * Set max listeners
     * @param count
     * @returns {boolean}
     */
    setMaxListeners(count){
        if(typeof count !== 'number'){
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
     */
    off(name, callback){
        const listenerList = this.__listenerMap[name];
        if(!listenerList || typeof callback !== 'function'){
            return this;
        }
        for(let i = listenerList.length - 1; i >= 0; i--){
            if(listenerList[i].callback === callback){
                listenerList.splice(i, 1);
            }
        }
        if(!listenerList.length){
            delete this.__listenerMap[name];
        }
        return this;
    }

    /**
     * Delete all EE listener
     * @param name
     * @return {EventEmitter}
     */
    offAll(name){
        delete this.__listenerMap[name];
        return this;
    }

    /**
     * Emit EE listener
     * @param name
     * @param argList
     * @return {EventEmitter}
     */
    emit(name, ...argList){
        let listenerList = this.__listenerMap[name];
        if(!listenerList){
            return this;
        }
        listenerList = listenerList.slice();
        for(let i = 0; i < listenerList.length; i++){
            const currentList = this.__listenerMap[name];
            const index = currentList ? currentList.indexOf(listenerList[i]) : -1;
            if(index === -1){
                continue;
            }
            if(listenerList[i].once){
                currentList.splice(index, 1);
                if(!currentList.length){
                    delete this.__listenerMap[name];
                }
            }
            listenerList[i].callback.apply(this, argList);
        }
        return this;
    }
}

const MSG_SO_UPDATE = 0;
const MSG_SD_CREATE = 1;
const MSG_SD_UPDATE = 2;
const MSG_SO_TIME   = 3;

/**
 *
 * @returns {Window|*}
 */
const getWindow = () => xSocket.window;

/**
 *
 * @returns {any}
 */
const getProcess = () => xSocket.process;

/**
 *
 * @param min
 * @param max
 * @returns {number}
 */
const getRandInt = (min, max) => Math.floor(min + Math.random() * (max + 1 - min));

/**
 *
 * @param data
 * @returns {boolean}
 */
const isBinary = (data) => typeof ArrayBuffer === 'function' && (data instanceof ArrayBuffer || ArrayBuffer.isView(data));

/**
 *
 * @param type
 * @returns {number}
 */
const getMessageDataIndex = (type) => type === MSG_SD_CREATE ? 4 : 2;

/**
 *
 * @param text
 * @returns {Uint8Array}
 */
const encodeText = (text) => {
    if(typeof TextEncoder === 'function'){
        return new TextEncoder().encode(text);
    }
    const binary = unescape(encodeURIComponent(text));
    const byteList = new Uint8Array(binary.length);
    for(let i = 0; i < binary.length; i++){
        byteList[i] = binary.charCodeAt(i);
    }
    return byteList;
};

/**
 *
 * @param byteList
 * @returns {string}
 */
const decodeText = (byteList) => {
    if(typeof TextDecoder === 'function'){
        return new TextDecoder().decode(byteList);
    }
    let binary = '';
    for(let i = 0; i < byteList.length; i++){
        binary += String.fromCharCode(byteList[i]);
    }
    return decodeURIComponent(escape(binary));
};

/**
 *
 * @param body
 * @returns {string|Uint8Array}
 */
const encodeMessage = (body) => {
    const dataIndex = getMessageDataIndex(body[0]);
    const data = body[dataIndex];
    if(!isBinary(data)){
        return JSON.stringify(body);
    }
    body = body.slice();
    body[dataIndex] = null;
    const headerBytes = encodeText(JSON.stringify(body));
    const dataBytes = data instanceof ArrayBuffer ? new Uint8Array(data) : new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    const frame = new Uint8Array(4 + headerBytes.length + dataBytes.length);
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
const decodeMessage = (raw) => {
    let body;
    try{
        if(typeof raw === 'string'){
            body = JSON.parse(raw);
        }else {
            const frame = raw instanceof ArrayBuffer ? new Uint8Array(raw) : raw;
            const headerLength = new DataView(frame.buffer, frame.byteOffset, frame.byteLength).getUint32(0);
            body = JSON.parse(decodeText(frame.subarray(4, 4 + headerLength)));
            if(Array.isArray(body)){
                body[getMessageDataIndex(body[0])] = frame.subarray(4 + headerLength);
            }
        }
    }catch (e){
        return false;
    }
    return Array.isArray(body) && typeof body[0] === 'number' ? body : false;
};

var helpers = {
    getWindow,
    getProcess,
    getRandInt,
    isBinary,
    getMessageDataIndex,
    encodeText,
    decodeText,
    encodeMessage,
    decodeMessage,
    EventEmitter
};

class Data extends EventEmitter {

    /**
     *
     * @param __isOutput
     * @param __object
     * @param __socketObject
     */
    constructor(__isOutput, __object, __socketObject){
        super();
        this.setMaxListeners(0);
        __object = typeof __object === 'object' && __object ? __object : {};

        this.__isOutput     = !!__isOutput;
        this.__socketObject = __socketObject || false;
        this.__ID           = typeof __object.ID === 'string' ? __object.ID : '';
        this.__type         = typeof __object.type === 'string' ? __object.type : '';
        this.__data         = typeof __object.data === 'object' ? __object.data : {};
        this.__ttl          = typeof __object.ttl === 'number' && __object.ttl % 1 === 0 && __object.ttl >= 0 ? __object.ttl : Data.DEFAULT_TTL;
        const now           = this.__socketObject ? this.__socketObject.getTime() : Date.now();
        this.__time         = typeof __object.time === 'number' && __object.time > 0 ? __object.time : now;
        this.__resData      = {};
        this.__error        = false;
        this.__status       = Data.STATUS_NEW;
        this.__destroy      = false;
        this.__ttlTimeout   = setTimeout(() => {
            this.destroy('ttl', true);
        }, Math.max(0, Math.min(this.__ttl, this.__ttl - (now - this.__time))));
    }

    /**
     *
     * @returns {string}
     */
    getID(){
        return String(this.__ID);
    }

    /**
     *
     * @returns {string}
     */
    getType(){
        return String(this.__type);
    }

    /**
     *
     * @returns {number}
     */
    getTTL(){
        return this.__ttl;
    }

    /**
     *
     * @returns {number}
     */
    getTime(){
        return this.__time;
    }

    /**
     *
     * @returns {{}}
     */
    getData(){
        return this.__data || {};
    }

    /**
     *
     * @param key
     * @param def
     * @returns {*}
     */
    getDataVal(key, def){
        return this.getData()[key] || def;
    }

    /**
     *
     * @returns {{}}
     */
    getResData(){
        return this.__resData || {};
    }

    /**
     *
     * @param key
     * @param def
     * @returns {*}
     */
    getResDataVal(key, def){
        return this.getResData()[key] || def;
    }

    /**
     *
     * @returns {number}
     */
    getStatus(){
        return this.__status;
    }

    /**
     *
     * @returns {boolean|string}
     */
    getDestroy(){
        return this.__destroy || false;
    }

    /**
     *
     * @returns {SocketObject|boolean}
     */
    getSocketObject(){
        return this.__socketObject;
    }

    /**
     *
     * @returns {string|boolean}
     */
    getError(){
        return this.__error || false;
    }

    /**
     *
     * @returns {boolean}
     */
    isNew(){
        return this.__status === Data.STATUS_NEW;
    }

    /**
     *
     * @returns {boolean}
     */
    isSend(){
        return this.__status === Data.STATUS_SEND;
    }

    /**
     *
     * @returns {boolean}
     */
    isResponse(){
        return this.__status === Data.STATUS_RESPONSE;
    }

    /**
     *
     * @param data
     * @param error
     * @returns {Promise<Data>}
     */
    response(data, error){
        const promise = new Promise((resolve, reject) => {
            if(this.__isOutput){
                return reject(new Error('Output data'));
            }
            if(this.__destroy || this.isResponse() || !this.__ttl){
                return reject(new Error('destroy'));
            }
            this.setResData(data);
            this.setError(error);
            this.once('response', () => {
                resolve(this);
            });
            this.once('destroy', (msg) => {
                reject(new Error(msg));
            });
            this.__socketObject.getWsReady().then((ws) => {
                if(ws.sendObject([
                    MSG_SD_UPDATE,
                    Number(this.__ID),
                    this.getResData(),
                    this.getError()
                ])){
                    this.setStatus(Data.STATUS_RESPONSE);
                }else {
                    this.destroy('Send failed');
                }
            }).catch((e) => {
                this.destroy(e.message || 'Send failed');
            });
        });
        promise.catch(() => {});
        return promise;
    }

    /**
     *
     * @param status
     * @returns {boolean}
     */
    setStatus(status){
        if(
            (status !== Data.STATUS_SEND && status !== Data.STATUS_RESPONSE) ||
            this.__status === Data.STATUS_RESPONSE ||
            this.__status === status
        ){
            return false;
        }
        this.__status = status;
        this.emit('setStatus', status);
        if(status === Data.STATUS_RESPONSE){
            this.emit('response', this.getResData(), this.getError());
            this.destroy('response');
        }
        return true;
    }

    /**
     *
     * @param errorMsg
     * @returns {boolean}
     */
    setError(errorMsg){
        if(typeof errorMsg !== 'string'){
            return false;
        }
        this.__error = errorMsg;
        return true;
    }

    /**
     *
     * @param data
     * @returns {boolean}
     */
    setResData(data){
        if(typeof data !== 'object'){
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
     */
    destroy(msg, isSetError){
        if(this.__destroy){
            return false;
        }
        this.__destroy = typeof msg === 'string' && msg.length ? msg : 'destroy';
        clearTimeout(this.__ttlTimeout);
        if(isSetError && !this.__error){
            this.setError(this.__destroy);
        }
        if(this.__status !== Data.STATUS_RESPONSE){
            this.setStatus(Data.STATUS_RESPONSE);
        }
        this.emit('destroy', this.__destroy);
        this.clearAllListerens();
        return true;
    }
}
Data.STATUS_NEW      = 0;
Data.STATUS_SEND     = 1;
Data.STATUS_RESPONSE = 2;
Data.DEFAULT_TTL     = 3000;

class SocketObject extends EventEmitter {

    /**
     *
     * @param __ServerConfigure
     * @param __req
     */
    constructor(__ServerConfigure, __req){
        super();
        this.setMaxListeners(0);

        this.__serverConfigure = __ServerConfigure;
        this.__req             = __req;
        this.__ws              = undefined;
        this.__ID              = '';
        this.__sign            = '';
        this.__destroy         = undefined;
        this.__isNewConnection = true;
        this.__socketDataMap   = {};
        this.__socketDataInc   = 0;
        this.__query           = {};
        this.__timeOffset      = 0;

        this.on('destroy', this.__onDestroy);
        this.on('ws|disconnect', this.__onWsDisconnect);
        this.on('ws|connect', this.__onWsConnect);
        this.on('ws|message', this.__onWsMessage);
    }

    /**
     *
     * @returns {string}
     */
    getID(){
        return this.__ID || '';
    }

    /**
     *
     * @returns {string}
     */
    getSign(){
        return this.__sign || '';
    }

    /**
     *
     * @returns {number}
     */
    getTime(){
        return Date.now() + this.__timeOffset;
    }

    /**
     *
     * @returns {number}
     */
    getTimeOffset(){
        return this.__timeOffset;
    }

    /**
     *
     * @param timeOffset
     * @returns {boolean}
     */
    setTimeOffset(timeOffset){
        if(typeof timeOffset !== 'number' || timeOffset !== timeOffset){
            return false;
        }
        this.__timeOffset = Math.round(timeOffset);
        return true;
    }

    /**
     *
     * @returns {*|{}}
     */
    getReq(){
        return this.__req || {};
    }

    /**
     *
     * @returns {*}
     */
    getQuery(){
        return typeof this.__query === 'object' ? this.__query : {};
    }

    /**
     *
     * @param key
     * @param def
     * @returns {*}
     */
    getQueryVal(key, def){
        return this.getQuery()[key] || def;
    }

    /**
     *
     * @returns {*|{}}
     */
    getReqHeaders(){
        return this.getReq()['headers'] || {};
    }

    /**
     *
     * @returns {*|string}
     */
    getOrigin(){
        return this.getReqHeaders()['origin'] || 'unknown';
    }

    /**
     *
     * @returns {string}
     */
    getIP(){
        return String((this.getReq()['connection'] || {})['remoteAddress'] || 'unknown');
    }

    /**
     *
     * @returns {{}}
     */
    getSocketDataList(){
        return this.__socketDataMap;
    }

    /**
     *
     * @param ID
     * @returns {Data|boolean}
     */
    getSocketData(ID){
        return this.__socketDataMap[ID] || false;
    }

    /**
     *
     * @returns {boolean}
     */
    isConnection(){
        return typeof this.__ws === 'object' ? this.__ws.isConnection() : false;
    }

    /**
     *
     * @returns {any|boolean}
     */
    getConnection(){
        return this.isConnection() ? this.__ws : false;
    }

    /**
     *
     * @returns {string|boolean}
     */
    getDestroy(){
        return this.__destroy || false;
    }

    /**
     *
     * @returns {boolean}
     */
    isDestroy(){
        return !!this.getDestroy();
    }

    /**
     *
     * @returns {boolean}
     */
    isServer(){
        return !!this.__serverConfigure;
    }

    /**
     *
     * @returns {Promise}
     */
    getWsReady(){
        return new Promise((resolve, reject) => {
            if(this.getConnection()){
                return resolve(this.getConnection());
            }
            const destroyHandler = () => {
                this.off('connect', connectHandler);
                reject(new Error('destroy'));
            };
            const connectHandler = () => {
                this.off('destroy', destroyHandler);
                if(this.getConnection()){
                    resolve(this.getConnection());
                }else {
                    reject(new Error('Connection failed'));
                }
            };
            this.once('connect', connectHandler);
            this.once('destroy', destroyHandler);
        });
    }

    /**
     *
     * @param type
     * @param data
     * @param ttl
     * @returns {Promise<Data>}
     */
    send(type, data, ttl){
        const socketData = new Data(true, {
            'ID' : String(++this.__socketDataInc),
            'type' : type,
            'data' : data,
            'ttl' : ttl,
            'time' : this.getTime()
        }, this);
        if(socketData.getTTL()){
            this.__socketDataMap[socketData.getID()] = socketData;
            socketData.once('destroy', () => {
                delete this.__socketDataMap[socketData.getID()];
            });
        }
        this.getWsReady().then((ws) => {
            if(!ws.sendObject([
                MSG_SD_CREATE,
                Number(socketData.getID()),
                socketData.getType(),
                socketData.getTTL(),
                socketData.getData(),
                socketData.getTime()
            ])){
                return socketData.destroy('Send failed');
            }
            socketData.setStatus(socketData.getTTL() ? Data.STATUS_SEND : Data.STATUS_RESPONSE);
        }).catch((e) => {
            socketData.destroy(e.message || 'Send failed');
        });
        return Promise.resolve(socketData);
    }

    /**
     *
     * @param type
     * @param data
     * @param ttl
     * @returns {Promise<Data>}
     */
    sendReadyResponse(type, data, ttl){
        return this.send(type, data, ttl).then((socketData) => new Promise((resolve, reject) => {
            const responseHandler = () => {
                if(socketData.getError()){
                    return reject(new Error(socketData.getError()));
                }
                resolve(socketData);
            };
            if(socketData.isResponse() || socketData.getDestroy()){
                return responseHandler();
            }
            socketData.once('response', responseHandler);
        }));
    }

    /**
     *
     * @param type
     * @param data
     * @param ttl
     * @returns {Promise<Data>}
     */
    sendReady(type, data, ttl){
        return this.sendReadyResponse(type, data, ttl);
    }

    /**
     *
     * @param msg
     * @returns {boolean}
     */
    destroy(msg){
        if(this.isDestroy()){
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
     */
    disconnect(msg){
        const ws = this.getConnection();
        if(!ws){
            return false;
        }
        return ws.closeConnection(msg);
    }

    /**
     *
     * @param ws
     * @returns {boolean}
     */
    update(ws){
        if(typeof ws !== 'object' || typeof ws.isConnection !== 'function' || typeof ws.query !== 'object'){
            return false;
        }
        if(!ws.isConnection() || this.__destroy){
            return false;
        }
        if(this.__ID !== ws.query['xSOId'] || this.__sign !== ws.query['xSOSign']){
            this.__ID    = ws.query['xSOId'];
            this.__sign  = ws.query['xSOSign'];
            this.__query = Object(ws['query'] || {});
            delete this.__query['xSOId'];
            delete this.__query['xSOSign'];
            delete this.__query['xV'];
            this.emit('update', this);
        }
        if(this.isServer()){
            ws.sendObject([
                MSG_SO_UPDATE,
                this.getID(),
                this.getSign(),
                typeof this.__serverConfigure === 'object' ? this.__serverConfigure : {},
                xSocket.VERSION,
                Date.now()
            ]);
        }else {
            this.setTimeOffset(ws.timeOffset);
            ws.sendObject([MSG_SO_TIME, Date.now()]);
        }
        this.__ws = ws;
        ws.SocketObject = this;
        this.emit('ws|connect', ws);
        return true;
    }

    __onDestroy(){
        for(const ID in this.__socketDataMap){
            this.__socketDataMap[ID].destroy('SocketObject|destroy');
        }
    }

    __onWsDisconnect(msg){
        msg = typeof msg === 'string' ? msg : 'unknown';
        this.emit('disconnect', this, msg);
        if(msg === 'end'){
            this.destroy('end');
        }
    }

    __onWsConnect(){
        this.emit('connect', this);
        if(!this.__isNewConnection){
            this.emit('reconnect', this);
        }else {
            this.__isNewConnection = false;
        }
    }

    __onWsMessage(body){
        if(body[0] === MSG_SO_TIME){
            return this.__onWsTime(body);
        }
        const ID = body[1];
        if(typeof ID !== 'number' || ID % 1 !== 0 || ID < 1){
            return;
        }
        let socketData;
        if(body[0] === MSG_SD_CREATE){
            if(typeof body[2] !== 'string' || !body[2].length || typeof body[4] !== 'object'){
                return;
            }
            socketData = new Data(false, {
                'ID' : String(ID),
                'type' : body[2],
                'ttl' : body[3],
                'data' : body[4],
                'time' : body[5]
            }, this);
            this.emit('data', socketData);
            this.emit('data|' + socketData.getType(), socketData);
            if(!socketData.getTTL()){
                socketData.setStatus(Data.STATUS_RESPONSE);
            }
        }else if(body[0] === MSG_SD_UPDATE){
            socketData = this.getSocketData(String(ID));
            if(socketData){
                socketData.setResData(body[2]);
                socketData.setError(body[3] || false);
                socketData.setStatus(Data.STATUS_RESPONSE);
            }
        }
    }

    __onWsTime(body){
        const ws = this.getConnection();
        if(!ws || typeof body[1] !== 'number'){
            return;
        }
        if(this.isServer()){
            ws.sendObject([MSG_SO_TIME, body[1], Date.now()]);
        }else if(typeof body[2] === 'number'){
            const now = Date.now();
            this.setTimeOffset(body[2] + (now - body[1]) / 2 - now);
        }
    }
}
SocketObject.MSG_SO_UPDATE = MSG_SO_UPDATE;
SocketObject.MSG_SD_CREATE = MSG_SD_CREATE;
SocketObject.MSG_SD_UPDATE = MSG_SD_UPDATE;
SocketObject.MSG_SO_TIME   = MSG_SO_TIME;

class Client extends EventEmitter {

    /**
     *
     * @param serverUrl
     * @param query
     * @param settings
     */
    constructor(serverUrl, query, settings){
        super();
        this.setMaxListeners(0);

        this.__urlList          = [];
        this.__ws               = undefined;
        this.__socketObject     = false;
        this.__isVersionWarned  = false;
        this.__settings         = typeof settings === 'object' && settings ? settings : {};
        this.__query            = typeof query === 'object' && query ? query : {};

        if(typeof serverUrl === 'object' && serverUrl){
            for(const i in serverUrl){
                this.__urlList.push(serverUrl[i]);
            }
        }else if(typeof serverUrl === 'string'){
            this.__urlList.push(serverUrl);
        }
        if(!this.__urlList.length){
            throw new Error('Invalid urlList');
        }
        const reconnect = () => {
            setTimeout(() => {
                this.getWs().catch(() => {});
            }, 1000);
        };
        this.on('ws|disconnect', reconnect);
        this.on('ws|error', reconnect);
        const closeConnection = () => {
            try{
                this.__ws.closeConnection('end');
            }catch (e){}
        };
        if(getWindow()){
            getWindow().addEventListener('pagehide', closeConnection);
            getWindow().addEventListener('unload', closeConnection);
        }else if(getProcess()){
            getProcess().on('exit', closeConnection);
        }
        this.on('connect', (socketObject) => {
            let destroyTimeout = false;
            const setDestroyTimeout = (action, ttl) => {
                if(destroyTimeout){
                    clearTimeout(destroyTimeout);
                    destroyTimeout = false;
                }
                if(action){
                    ttl = typeof ttl === 'number' && !isNaN(ttl) && ttl > 0 ? ttl : 0;
                    destroyTimeout = setTimeout(() => {
                        socketObject.destroy('socketObjectTimeout');
                        destroyTimeout = false;
                    }, ttl);
                }
            };
            socketObject.on('reconnect', () => {
                setDestroyTimeout(false);
            });
            socketObject.on('disconnect', () => {
                const socketObjectTimeout = parseInt(Object(socketObject.getQueryVal('configure', {}))['socketObjectTimeout'] || 0);
                if(typeof socketObjectTimeout === 'number'){
                    setDestroyTimeout(true, socketObjectTimeout);
                }
            });
            socketObject.once('destroy', () => {
                setDestroyTimeout(false);
            });
        });
        this.getWs().catch(() => {});
    }

    /**
     *
     * @returns {{}}
     */
    getSettings(){
        return this.__settings || {};
    }

    /**
     *
     * @param key
     * @param def
     * @returns {*}
     */
    getSettingsVal(key, def){
        return this.getSettings()[key] || def;
    }

    /**
     *
     * @returns {SocketObject|boolean}
     */
    getSocketObject(){
        return this.__socketObject;
    }

    /**
     *
     * @returns {Promise<SocketObject>}
     */
    getSocketObjectReady(){
        return new Promise((resolve) => {
            if(this.getSocketObject()){
                return resolve(this.getSocketObject());
            }
            this.once('connect', (socketObject) => {
                resolve(socketObject);
            });
        });
    }

    /**
     *
     * @returns {Promise<{}>}
     */
    getWs(){
        return new Promise((resolve) => {
            if(typeof this.__ws === 'object'){
                return resolve(this.__ws);
            }
            this.__ws = null;
            let url = this.__urlList[getRandInt(0, this.__urlList.length - 1)];
            url += (url.indexOf('?') === -1 ? '?' : '&');

            const query = this.__query;
            query['xV'] = xSocket.VERSION;
            if(this.__socketObject && this.__socketObject.getID().length && this.__socketObject.getSign().length){
                query['xSOId'] = String(this.__socketObject.getID());
                query['xSOSign'] = String(this.__socketObject.getSign());
            }
            for(const i in query){
                try{
                    url += encodeURIComponent(String(i)) + '=' + encodeURIComponent(String(query[i])) + '&';
                }catch (e){}
            }

            const ws = new xSocket.WebSocket(url);
            ws.binaryType = 'arraybuffer';
            ws.isConnection = () => ws.readyState === 1;
            ws.closeConnection = (msg) => {
                if(ws.isConnection()){
                    try{
                        ws.close(1000, String(msg || 'close'));
                        return true;
                    }catch (e){
                        console.error(e);
                    }
                }
                return false;
            };
            ws.sendMessage = (msg) => {
                try{
                    if(ws.isConnection()){
                        ws.send(String(msg));
                        return true;
                    }
                }catch (e){}
                return false;
            };
            ws.sendObject = (object) => {
                try{
                    if(ws.isConnection()){
                        ws.send(encodeMessage(object));
                        return true;
                    }
                }catch (e){}
                return false;
            };
            let closeMsg;
            let msgInc = 0;
            let pongInterval = false;
            ws.onopen = () => {
                this.__ws = ws;
                this.emit('ws|connect', ws);
                pongInterval = setInterval(() => {
                    if(!msgInc){
                        ws.closeConnection('pingTimeout');
                    }
                    msgInc = 0;
                }, 21000);
                resolve(ws);
            };
            ws.onmessage = (e) => {
                msgInc++;
                if(e.data === 'ping'){
                    return ws.sendMessage('pong');
                }
                try{
                    const body = decodeMessage(e.data);
                    if(!body){
                        return;
                    }
                    if(body[0] === MSG_SO_UPDATE){
                        this.__onSocketObjectUpdate(ws, body);
                    }else if(this.__socketObject){
                        this.__socketObject.emit('ws|message', body);
                    }
                }catch (e){}
            };
            ws.onclose = (e) => {
                if(pongInterval){
                    clearInterval(pongInterval);
                }
                this.__ws = undefined;
                closeMsg = closeMsg || e.reason || 'close';
                this.emit('ws|disconnect', closeMsg);
                if(this.__socketObject){
                    this.__socketObject.emit('ws|disconnect', closeMsg);
                }
            };
            ws.onerror = (e) => {
                closeMsg = e.message;
                this.emit('ws|error', e.message, ws);
            };
        });
    }

    /**
     *
     * @param type
     * @param data
     * @param ttl
     * @returns {Promise<Data>}
     */
    send(type, data, ttl){
        return this.getSocketObjectReady().then((socketObject) => socketObject.send(type, data, ttl));
    }

    /**
     *
     * @param type
     * @param data
     * @param ttl
     * @returns {Promise<Data>}
     */
    sendReady(type, data, ttl){
        return this.getSocketObjectReady().then((socketObject) => socketObject.sendReady(type, data, ttl));
    }

    /**
     *
     * @param type
     * @param data
     * @param ttl
     * @returns {Promise<Data>}
     */
    sendReadyResponse(type, data, ttl){
        return this.sendReady(type, data, ttl);
    }

    /**
     *
     * @param msg
     * @returns {boolean}
     */
    disconnect(msg){
        const socketObject = this.getSocketObject();
        if(!socketObject){
            return false;
        }
        return socketObject.disconnect(msg);
    }

    __onSocketObjectUpdate(ws, body){
        if(body[4] !== xSocket.VERSION && !this.__isVersionWarned){
            this.__isVersionWarned = true;
            console.warn('[warn|xSocket]', 'Version mismatch', 'client ' + xSocket.VERSION + ', server ' + (body[4] || 'unknown'));
        }
        ws.timeOffset = typeof body[5] === 'number' ? body[5] - Date.now() : 0;
        ws.query = {
            'xSOId' : String(body[1]),
            'xSOSign' : String(body[2]),
            'configure' : typeof body[3] === 'object' ? body[3] : {}
        };
        if(this.__socketObject){
            if(ws.query['xSOId'] === this.__socketObject.getID() && ws.query['xSOSign'] === this.__socketObject.getSign() && this.__socketObject.update(ws)){
                return;
            }
            this.__socketObject.destroy('update');
        }
        const socketObject = this.__socketObject = new SocketObject();
        socketObject.once('connect', () => {
            this.emit('connect', socketObject);
        });
        socketObject.on('destroy', (socketObject, msg) => {
            this.__socketObject = false;
            this.emit('destroy', socketObject, msg);
        });
        socketObject.on('disconnect', (socketObject, msg) => {
            this.emit('disconnect', socketObject, msg);
        });
        socketObject.on('reconnect', () => {
            this.emit('reconnect', socketObject);
        });
        socketObject.on('data', (socketData) => {
            this.emit('data', socketData);
        });
        if(!socketObject.update(ws)){
            ws.closeConnection('update');
        }
    }
}

xSocket.helpers       = helpers;
xSocket.EventEmitter  = EventEmitter;
xSocket.Data          = Data;
xSocket.xSocketObject = SocketObject;
xSocket.Client        = Client;

const VERSION = xSocket.VERSION;

export { Client, Data, EventEmitter, SocketObject, VERSION, xSocket as default, helpers };
