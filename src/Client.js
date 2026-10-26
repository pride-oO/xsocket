import xSocket from './xSocket.js';
import EventEmitter from './EventEmitter.js';
import SocketObject from './SocketObject.js';
import { MSG_SO_UPDATE, getWindow, getProcess, getRandInt, encodeMessage, decodeMessage } from './helpers.js';

export default class Client extends EventEmitter {

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
