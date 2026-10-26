import EventEmitter from './EventEmitter.js';
import { MSG_SD_UPDATE } from './helpers.js';

export default class Data extends EventEmitter {

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
                }else{
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
