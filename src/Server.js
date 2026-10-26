import events from 'events';
import ws from 'ws';
import http from 'http';
import https from 'https';
import fs from 'fs';
import crypto from 'crypto';
import xSocket from './xSocket.js';
import xSocketObject from './SocketObject.js';
import { encodeMessage, decodeMessage } from './helpers.js';

export default class Server extends events {

    constructor(settings) {
        super();
        this.__settings = Object.assign({}, settings || {});
        this.__http = undefined;
        this.__ws   = undefined;
        this.__isListen = undefined;
        this.__useCallback = {};
        this.__socketObjectList = {};
        this.__versionWarnMap = {};
        this.__socketObjectTimeout = parseInt(this.getSettingsVal('socketObjectTimeout', 0));
        if(this.__socketObjectTimeout !== this.__socketObjectTimeout || this.__socketObjectTimeout < 0){
            this.__socketObjectTimeout = 0;
            console.warn('[warn|xSocket]', 'socketObjectTimeout invalid value,', 'set default is 0');
        }
    }

    /**
     *
     * @returns {{}}
     */
    getHttpsOptions(){
        const options = Object.assign({}, this.getSettingsVal('https', {}));
        if(typeof options['cert'] === 'string'){
            options['cert'] = fs.readFileSync(options['cert']);
        }
        if(typeof options['key'] === 'string'){
            options['key'] = fs.readFileSync(options['key']);
        }
        return options;
    }

    /**
     *
     * @returns {boolean}
     */
    isHttps(){
        return !!this.getSettingsVal('https', {})['cert'];
    }

    /**
     *
     * @returns {boolean}
     */
    isListen(){
        return this.__isListen === true;
    }

    /**
     *
     * @returns {string}
     */
    getHost(){
        return this.getSettingsVal('host', undefined);
    }

    /**
     *
     * @param host
     * @returns {Server}
     */
    setHost(host){
        return this.setSettingsVal('host', host);
    }

    /**
     *
     * @param port
     * @returns {Server}
     */
    setPort(port){
        return this.setSettingsVal('port', port);
    }

    /**
     *
     * @param options
     * @returns {Server}
     */
    setHttps(options){
        this.setSettingsVal('https', options);
        if(this.isListen() && this.isHttps() && this.__http instanceof https.Server){
            this.__http.setSecureContext(this.getHttpsOptions());
        }
        return this;
    }

    /**
     *
     * @param key
     * @param val
     * @returns {Server}
     */
    setSettingsVal(key, val){
        this.__settings[key] = val;
        return this;
    }

    __createServer(){
        const $this = this;
        const SocketObjectTimeout = this.__socketObjectTimeout;
        const requestHandler = (req, res) => {
            $this.emit('https', req, res);
            setTimeout(() => {
                try{
                    res.writeHead(404);
                    res.end();
                }catch (e){}
            }, 5000);
        };
        this.__http = this.isHttps() ? https.createServer(this.getHttpsOptions(), requestHandler) : http.createServer(requestHandler);
        let perMessageDeflate = $this.getSettingsVal('perMessageDeflate', false);
        if(perMessageDeflate === true){
            perMessageDeflate = {
                'threshold' : 1024,
                'serverNoContextTakeover' : true,
                'clientNoContextTakeover' : true
            };
        }
        this.__ws = new ws.Server({'server' : this.__http, 'perMessageDeflate' : perMessageDeflate});
        this.__ws.on('error', () => {});
        this.__ws.on('connection', (wsClient, req) => {
            wsClient.isConnection = () => {
                return wsClient.readyState === 1;
            };
            wsClient.closeConnection = (msg) => {
                try{
                    wsClient.close(1000, String(msg || 'close'));
                }catch (e){}
            };

            wsClient.sendMessage = (msg) => {
                try {
                    if(wsClient.isConnection()){
                        wsClient.send(String(msg));
                        return true;
                    }
                }catch (e){}
                return false;
            };
            wsClient.sendObject = (object) => {
                try {
                    if(wsClient.isConnection()){
                        wsClient.send(encodeMessage(object));
                        return true;
                    }
                }catch (e){}
                return false;
            };
            wsClient.query = ((str) => {
                let queryString = typeof str === 'string' ? str.replace(/.*?\?/,"") : '';
                let query      = {};
                if (!queryString.length) return query;
                let split = queryString.split('&');
                for (let i in split)
                {
                    try{
                        let key = decodeURIComponent(split[i].split('=')[0]);
                        if (!key.length) continue;
                        let val = decodeURIComponent(split[i].split('=')[1]) || undefined;
                        let valArr = typeof val === 'string' ? val.split('#') : [].push(val);
                        query[key] = valArr[0];
                        if(valArr[1]){
                            query['_hashTag_'] = valArr[1];
                        }
                    }catch (e){}
                }
                return query;
            })(req.url || '');
            const clientVersion = String(wsClient.query['xV'] || 'unknown');
            if(clientVersion !== xSocket.VERSION && !$this.__versionWarnMap[clientVersion]){
                $this.__versionWarnMap[clientVersion] = true;
                console.warn('[warn|xSocket]', 'Version mismatch', 'server ' + xSocket.VERSION + ', client ' + clientVersion);
            }
            let SocketObject;
            wsClient.query['xSOId']   = String(wsClient.query['xSOId'] || '');
            wsClient.query['xSOSign'] = String(wsClient.query['xSOSign'] || '');
            if(wsClient.query['xSOId'].length && wsClient.query['xSOSign'].length){
                let s = $this.getSocketObject(wsClient.query['xSOId']);
                if(s && s.getSign() === wsClient.query['xSOSign'] && s.update(wsClient)){
                    SocketObject = s;
                }
            }
            if(!SocketObject){
                wsClient.query['xSOId'] = crypto.randomUUID();
                wsClient.query['xSOSign'] = crypto.randomUUID();
                SocketObject = new xSocketObject({'socketObjectTimeout' : SocketObjectTimeout}, req);
                const isAuth = $this.getSettingsVal('auth', false);
                if(!isAuth && typeof $this.__useCallback['auth'] !== 'function'){
                    $this.use('auth', (SocketObject, successCallback) => {
                        successCallback();
                    });
                }
                
                if(SocketObject.update(wsClient)){
                    $this.useEmit('auth', SocketObject).then(() => {
                        $this.__socketObjectList[SocketObject.getID()] = SocketObject;
                        $this.emit('connect', SocketObject);
                        let soTimeout = undefined;
                        SocketObject.on('disconnect', (SocketObject) => {
                            soTimeout = setTimeout((SocketObject) => {
                                SocketObject.destroy('socketObjectTimeout');
                            }, SocketObjectTimeout, SocketObject);
                            SocketObject.once('connect', () => {
                                if(soTimeout){
                                    try{
                                        clearTimeout(soTimeout);
                                        soTimeout = undefined;
                                    }catch (e){}
                                }
                            });
                        });
                        SocketObject.once('destroy', (SocketObject) => {
                            wsClient.closeConnection(SocketObject.getDestroy() || 'destroy');
                            delete $this.__socketObjectList[SocketObject.getID()];
                            if(soTimeout){
                                try{
                                    clearTimeout(soTimeout);
                                    soTimeout = undefined;
                                }catch (e){}
                            }
                        });
                    }).catch((e) => {
                        SocketObject.destroy(e.message || 'deniedAuth');
                    });
                }else{
                    wsClient.closeConnection('update');
                }
            }
            let msgInc = 0;
            let countPing = 0;
            const pingTimeout = setInterval(() => {
                if(countPing > 4){
                    if(!msgInc){
                        return wsClient.closeConnection('pingTimeout');
                    }
                    msgInc = 0;
                    countPing = 0;
                }
                wsClient.sendMessage('ping');
                countPing++;
            }, 5000);
            wsClient.onmessage = (e) => {
                msgInc++;
                if(e.data === 'pong' || !SocketObject){
                    return;
                }
                try{
                    const body = decodeMessage(e.data);
                    if(body){
                        SocketObject.emit('ws|message', body);
                    }
                }catch (e){}
            };
            wsClient.onclose = (e) => {
                try{clearInterval(pingTimeout); }catch (e){}
                let msg = e.reason || 'close';
                if(SocketObject){
                    SocketObject.emit('ws|disconnect', msg);
                }
                $this.emit('ws|disconnect', msg);

            };
            $this.emit('ws|connect', wsClient);
        });
    }

    /**
     *
     * @returns {{}}
     */
    getSocketObjectList(){
        return this.__socketObjectList || {};
    }

    /**
     *
     * @param ID
     * @returns {*|boolean}
     */
    getSocketObject(ID){
        return this.getSocketObjectList()[ID] || false;
    }

    /**
     *
     * @returns {number}
     */
    getPort(){
        return this.getSettings()['port'] || (this.isHttps() ? 443 : 80);
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
     * @param target
     * @param data
     * @returns {Promise}
     */
    useEmit(target, data){
      const $this = this;
      return new Promise((resolve, reject) => {
          const callback = typeof $this.__useCallback[target] == 'function' ? $this.__useCallback[target] : false;
          let timeout = false;
          const successCallback = (data, data1, data2) => {
              try{
                  if(timeout){
                      clearTimeout(timeout);
                      timeout = false;
                  }
              }catch (e){}
              resolve(data, data1, data2);
          };
          const failedCallback = (message) => {
              try{
                  if(timeout){
                      clearTimeout(timeout);
                      timeout = false;
                  }
              }catch (e){}
              reject(new Error(typeof message === 'string' ? message : ''));
          };
          timeout = setTimeout((failedCallback) => {
                if(typeof failedCallback === 'function'){
                    failedCallback('timeout');
                }
          }, 10000, failedCallback);
          if(!callback){
              return failedCallback('callbackNotFound');
          }
          try{
              callback.call($this, data, successCallback, failedCallback);
          }catch (e){
              failedCallback(e.message);
          }
          
      });
    }

    /**
     *
     * @param target
     * @param callback
     * @returns {Server}
     */
    use(target, callback){
        if(typeof target !== 'string' || target.length < 1){
            throw new Error('Invalid target value!');
        }
        if(typeof callback !== 'function'){
            throw new Error('Invalid callback!');
        }
        this.__useCallback[target] = callback;
        this.emit('use|append', target, callback);
        return this;
    }

    /**
     *
     * @returns {Promise}
     */
    listen(){
        const $this = this;
        return new Promise((resolve, reject) => {
            if($this.__isListen){
                return reject(new Error('repeat'))
            }
            if($this.__isListen === null){
                $this.once('listen', () => {
                   resolve();
                });
                $this.once('error', (e) => {
                    reject(e);
                });
                return;
            }
            $this.__isListen = null;
            const errorHandler = (e) => {
                $this.__isListen = false;
                try{ $this.__ws.close(); }catch (e){}
                $this.__http = undefined;
                $this.__ws   = undefined;
                $this.emit('error', e);
                reject(e);
            };
            try{
                $this.__createServer();
                $this.__http.once('error', errorHandler);
                $this.__http.listen($this.getPort(), $this.getHost(), () => {
                    $this.__http.off('error', errorHandler);
                    $this.__http.on('error', (e) => {
                        $this.emit('error', e);
                    });
                    $this.__isListen = true;
                    $this.emit('listen');
                    resolve();
                });
            }catch (e){
                errorHandler(e);
            }
        });
    }

    /**
     *
     * @returns {Promise}
     */
    close(){
        const $this = this;
        return new Promise((resolve) => {
            const httpServer = $this.__http;
            const wsServer   = $this.__ws;
            $this.__http = undefined;
            $this.__ws   = undefined;
            $this.__isListen = false;
            for(const ID of Object.keys($this.__socketObjectList)){
                $this.__socketObjectList[ID].destroy('close');
            }
            if(!httpServer){
                return resolve();
            }
            for(const wsClient of wsServer.clients){
                wsClient.closeConnection('close');
            }
            wsServer.close(() => {
                httpServer.close(() => {
                    $this.emit('close');
                    resolve();
                });
                if(typeof httpServer.closeAllConnections === 'function'){
                    httpServer.closeAllConnections();
                }
            });
        });
    }

}
