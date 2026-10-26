import xSocket from './xSocket.js';
import EventEmitter from './EventEmitter.js';
import Data from './Data.js';
import { MSG_SO_UPDATE, MSG_SD_CREATE, MSG_SD_UPDATE, MSG_SO_TIME } from './helpers.js';

export default class SocketObject extends EventEmitter {

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
                }else{
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
        }else{
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
        }else{
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
