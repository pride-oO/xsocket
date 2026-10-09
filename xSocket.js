/**
 * xSocket
 * @licence MIT
 * @author Vadim Pride (pride.mk.ua@gmail.com)
 * @link https://github.com/VadimPride
 */
var xSocket = function xSocket(){
    xSocket.Client.apply(this, arguments);
};
xSocket.window = window || false;
xSocket.WebSocket = WebSocket || false;
xSocket.process = false;

window.xSocket = xSocket;
xSocket.helpers = new function helpersObject (){

    /**
     *
     * @returns {Window|*}
     */
    this.getWindow = function (){
        return xSocket.window;
    };

    /**
     *
     * @returns {any}
     */
    this.getProcess = function (){
        return xSocket.process;
    };

    /**
     *
     * @param min
     * @param max
     * @returns {number}
     */
    this.getRandInt = function (min, max){
        return Math.floor(min + Math.random() * (max + 1 - min));
    }


    /**
     *
     * @constructor
     */
    this.EventEmitter = function() {
        var $this = this;
        var __listerens = {};
        var __incListeren = 0;
        var __maxListeners = 10;
        var __prefix_once = '_once';


        /**
         * Append EE listeren
         * @param name
         * @param callback
         * @param once
         * @return {EventEmitter}
         */
        this.on = function(name, callback, once) {
            if(typeof name !== 'string' || typeof callback !== 'function'){
                return $this;
            }
            ++__incListeren;

            var i = String(String(__incListeren) + (once === true ? __prefix_once : ''));
            if(typeof __listerens[name] !== 'object'){
                __listerens[name] = {};
            }
            __listerens[name][i] = callback;
            if(__maxListeners > 0 && $this.listenerCount(name) > __maxListeners){
                console.warn('EventEmitter ', name, ', exceeded maxListeners : ', __maxListeners + '/' + $this.listenerCount(name), '. Use EE.setMaxListeners(n)!');
            }
            return $this;
        };

        /**
         * Apend once EE listeren
         * @param name
         * @param callback
         * @return {EventEmitter}
         */
        this.once = function(name, callback){
            return $this.on(name, callback, true);
        };

        /**
         * Getter all listeren name
         * @returns {string[]}
         */
        this.getListerensName = function(){
            return Object.keys(__listerens);
        };

        /**
         * Clear all listeren
         * @returns {{}}
         */
        this.clearAllListerens = function(){
            var listerens = $this.getListerensName();
            for(var i in listerens){
                $this.offAll(listerens[i]);
            }
            return __listerens;
        };

        /**
         * Getter count Event listeren
         * @param name
         * @returns {number}
         */
        this.listenerCount = function(name){
            var count = 0;
            if(typeof name !== 'string' || typeof __listerens[name] !== 'object'){
                return count;
            }
            return Object.keys(__listerens[name]).length;
        };

        /**
         * Set max listeners
         * @param count
         * @returns {boolean}
         */
        this.setMaxListeners = function(count){
            if(typeof count !== 'number'){
                return false;
            }
            __maxListeners = parseInt(count);
            return true;
        };

        /**
         * Delete EventEmitter listeren
         * @param name
         * @param callback
         * @return {EventEmitter}
         */
        this.off = function(name, callback){

            if(typeof name !== 'string' || typeof callback !== 'function' || typeof __listerens[name] !== 'object'){
                return $this;
            }
            for(var i in __listerens[name]){
                if(__listerens[name][i] === callback){
                    delete __listerens[name][i];
                }
            }
            if(Object.keys(__listerens[name]).length === 0){
                delete __listerens[name];
            }
            return $this;
        };

        /**
         * Delete all EventEmitter listeren
         * @param name
         * @return {EventEmitter}
         */
        this.offAll = function(name){
            if(typeof __listerens[name] !== 'object'){
                return $this;
            }
            for(var i in __listerens[name]){
                delete __listerens[name][i];
            }
            delete __listerens[name];
            return $this;
        };

        /**
         * Emit EE listeren
         * @param name
         * @param data_1
         * @param data_2
         * @param data_3
         * @param data_4
         * @param data_5
         * @return {EventEmitter}
         */
        this.emit = function(name, data_1, data_2, data_3, data_4, data_5){
            if(typeof name !== 'string' || typeof __listerens[name] !== 'object'){
                return $this;
            }
            for(var i in __listerens[name]){
                if(typeof __listerens[name][i] === 'function'){
                    __listerens[name][i].call(this, data_1, data_2, data_3, data_4, data_5);
                    try{
                        if(i.indexOf(__prefix_once) >= 0){
                            $this.off(name, __listerens[name][i]);
                        }
                    }catch (e){}
                }
            }
            return $this;
        };
    };

    this.getUID = function (){
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c,r){return ('x'== c ? ( r = Math.random()*16|0) : (r&0x3|0x8)).toString(16);});
    };

    /**
     *
     * @param uid
     * @returns {boolean}
     */
    this.checkUID = function (uid){
        return !!(new RegExp("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")).test(String(uid));
    };
};

/**
 *
 * @param __isOutput
 * @param __object
 * @constructor
 */
xSocket.Data = function xSocketData(__isOutput, __object, __socketObject){
    __object = typeof __object === 'object' ? __object : {};
    xSocket.helpers.EventEmitter.call(this);
    this.setMaxListeners(0);

    var DEFAULT_TIMEOUT = 10000;
    var DEFAULT_TTL     = 3000;
    var $this     = this;
    this.__ID      = typeof __object.ID === 'string' ? __object.ID : '';
    this.__type    = typeof __object.type === 'string' ? __object.type : '';
    this.__data    = typeof __object.data === 'object' ? __object.data : {};
    this.__ttl     = typeof __object.ttl === 'number' && __object.ttl % 1 === 0  && __object.ttl >= 0 ? __object.ttl : DEFAULT_TTL;
    this.__resData = {};
    this.__error   = false;
    this.__status  = xSocket.Data.STATUS_NEW;
    this.__destroy = false;

    /**
     *
     * @returns {string}
     */
    this.getID = function (){
        return String(this.__ID);
    };

    /**
     *
     * @returns {string}
     */
    this.getType = function (){
        return String(this.__type);
    };

    /**
     *
     * @returns {number}
     */
    this.getTTL = function (){
        return this.__ttl;
    };

    /**
     *
     * @returns {{}}
     */
    this.getData = function (){
        return this.__data || {};
    };

    /**
     *
     * @param key
     * @param def
     * @returns {*}
     */
    this.getDataVal = function (key, def){
        return this.getData()[key] || def;
    };

    /**
     *
     * @returns {{}}
     */
    this.getResData = function (){
        return this.__resData || {};
    };

    /**
     *
     * @param key
     * @param def
     * @returns {*}
     */
    this.getResDataVal = function (key, def){
        return this.getResData()[key] || def;
    };

    /**
     *
     * @returns {number}
     */
    this.getStatus = function (){
        return parseInt(this.__status);
    };

    /**
     *
     * @returns {boolean|*|string}
     */
    this.getDestroy = function (){
        return this.__destroy || false;
    };
    
    /**
     *
     * @return {boolean}
     */
    this.getSocketObject = function (){
        return __socketObject || false;
    };

    /**
     *
     * @param data
     * @returns {Promise}
     */
    this.response = function (data){
        return new Promise(function (){
            throw new Error('This SocketData is output, use response event');
        });
    };

    /**
     *
     * @returns {string|boolean}
     */
    this.getError = function (){
        return this.__error || false;
    };

    /**
     *
     * @returns {boolean}
     */
    this.isNew = function (){
        return this.__status === xSocket.Data.STATUS_NEW;
    };

    /**
     *
     * @returns {boolean}
     */
    this.isSend = function (){
        return this.__status === xSocket.Data.STATUS_SEND;
    };

    /**
     *
     * @returns {boolean}
     */
    this.isResponse = function (){
        return this.__status === xSocket.Data.STATUS_RESPONSE;
    };

    /**
     *
     * @returns {Promise}
     */
    this.send = function (){
        return new Promise(function (resolve, reject) {
            if($this.getDestroy()){
                return reject(new Promise($this.getDestroy));
            }
            if(__isOutput) {
                $this.emit('send');
            }
            $this.on('response', function (data){
                resolve(data);
            });
            $this.on('destroy', function (e){
                reject(new Error(e));
            });
        });
    };


    /**
     *
     * @param status
     * @returns {boolean}
     */
    this.setStatus = function (status){
        if(
            [xSocket.Data.STATUS_SEND, xSocket.Data.STATUS_RESPONSE].indexOf(status) === -1 ||
            this.getStatus() === xSocket.Data.STATUS_RESPONSE ||
            this.getStatus() === status
        ){
            return false;
        }
        this.__status = status;
        this.emit('setStatus', status);
        return true;
    };

    /**
     *
     * @param errorMsg
     * @returns {boolean}
     */
    this.setError = function (errorMsg){
        if(typeof errorMsg !== 'string'){
            return false;
        }
        this.__error = String(errorMsg);
        return true;
    };

    /**
     *
     * @param data
     * @returns {boolean}
     */
    this.setResData = function (data){
        if (typeof data !== 'object') return false;
        this.__resData = data;
        return true;
    };

    /**
     *
     * @param msg
     * @param isSetError
     * @returns {boolean}
     */
    this.destroy = function (msg, isSetError){
        if(this.getDestroy()){
            return false;
        }
        this.__destroy = String(typeof msg === 'string' && msg.length ? msg :  'destroy');
        if(isSetError && !this.getError()){
            this.setError(this.__destroy);
        }
        if(this.getStatus() !== xSocket.Data.STATUS_RESPONSE){
            $this.setStatus(xSocket.Data.STATUS_RESPONSE);
        }
        this.emit('destroy', this.__destroy);
        // Clear all events
        this.clearAllListerens();
    };

    /**
     *
     */
    (function (){
        var ttlDestroy = setTimeout(function (){
            $this.destroy('ttl', true);
        }, $this.getTTL());
        var destroyTimeout = setTimeout(function ($this){
            $this.destroy('responseTimeout', true);
        }, $this.getTTL() > DEFAULT_TIMEOUT ? $this.getTTL() : DEFAULT_TIMEOUT, $this);

        $this.once('destroy', function (){
            try{
                clearTimeout(destroyTimeout);
            }catch (e){}
            try{
                clearTimeout(ttlDestroy);
            }catch (e){}
        });

        $this.on('setStatus', function (status){
            if(status === xSocket.Data.STATUS_RESPONSE){
                $this.emit('response', $this.getResData(), $this.getError());
                $this.destroy('response');
            }
        });
    })();

};
xSocket.Data.STATUS_NEW      = 0;
xSocket.Data.STATUS_SEND     = 1;
xSocket.Data.STATUS_RESPONSE = 2;

/**
 *
 * @param __ServerConfigure
 * @param __req
 */
xSocket.xSocketObject = function (__ServerConfigure, __req){

    // Extend
    xSocket.helpers.EventEmitter.call(this);
    this.setMaxListeners(0);

    var $this             = this;
    var __ws              = undefined;
    var __ID              = '';
    var __sign            = '';
    var __destroy         = undefined;
    var __isNewConnection = true;
    var __socketDataList      = {};
    var __query = {};

    /**
     *
     * @returns {string}
     */
    this.getID = function (){
        return __ID || '';
    };

    /**
     *
     * @returns {string}
     */
    this.getSign = function (){
        return __sign || '';
    };

    /**
     *
     * @returns {*|{}}
     */
    this.getReq = function (){
        return __req || {};
    };

    /**
     *
     * @returns {*}
     */
    this.getQuery = function (){
        return typeof __query === 'object' ? __query : {};
    }

    /**
     *
     * @param key
     * @param def
     * @returns {*}
     */
    this.getQueryVal = function (key, def){
        return this.getQuery()[key] || def;
    };

    /**
     *
     * @returns {*|{}}
     */
    this.getReqHeaders = function (){
        return this.getReq()['headers'] || {}
    };

    /**
     *
     * @returns {*|string}
     */
    this.getOrigin = function (){
        return this.getReqHeaders()['origin'] || 'unknown';
    };

    /**
     *
     * @returns {string}
     */
    this.getIP = function (){
        return String((this.getReq()['connection'] || {})['remoteAddress'] || 'unknown');
    };


    /**
     *
     * @returns {{}}
     */
    this.getSocketDataList = function (){
        return __socketDataList;
    };

    /**
     *
     * @param ID
     * @returns {*|boolean}
     */
    this.getSocketData = function (ID){
        return this.getSocketDataList()[ID] || false;
    };

    /**
     *
     * @returns {boolean}
     */
    this.isConnection = function (){
        return typeof __ws == 'object' ? __ws.isConnection() : false;
    };

    /**
     *
     * @returns {any|boolean}
     */
    this.getConnection = function (){
        return this.isConnection() ? __ws : false;
    };

    /**
     *
     * @returns {any|boolean}
     */
    this.getDestroy = function (){
        return __destroy || false;
    };

    /**
     *
     * @returns {boolean}
     */
    this.isDestroy = function (){
        return !!this.getDestroy();
    }

    /**
     *
     * @returns {Promise}
     */
    this.getWsReady = function (){
        return new Promise(function(resolve, reject) {
            if($this.getConnection()){
                return resolve($this.getConnection());
            }
            var destroyHandler = function(){
                reject(new Error('destroy'));
            };
            $this.on('connect', function(){
                if($this.getConnection()){
                    resolve($this.getConnection());
                }else{
                    reject(new Error('Failed connection #10fg42w'))
                }
                $this.off('destroy', destroyHandler);
            });
            $this.once('destroy', destroyHandler);
        });
    };

    /**
     *
     * @param type
     * @param data
     * @param ttl
     * @returns {Promise<xSocket.Data>}
     */
    this.send = function (type, data, ttl) {
        var sendPromise = new Promise(function (resolve, reject) {
            var socketData = new xSocket.Data(true, {
                'ID' : xSocket.helpers.getUID(),
                'type' : type,
                'data' : data,
                'ttl' : ttl
            }, $this);
            if(socketData.getTTL()){
                __socketDataList[socketData.getID()] = socketData;
                socketData.on('destroy', function (){
                    delete __socketDataList[socketData.getID()];
                });
            }
            resolve(socketData);
            $this.getWsReady().then(function (ws){
                if(!ws.sendObject(['SD|create', {
                        'ID' : socketData.getID(),
                        'type' : socketData.getType(),
                        'ttl' : socketData.getTTL(),
                        'data' : socketData.getData()
                }])){
                    return socketData.destroy('Failed send #54te8ta65');
                }
                socketData.setStatus(socketData.getTTL() ? xSocket.Data.STATUS_SEND : xSocket.Data.STATUS_RESPONSE);
            }).catch(function (e){
                socketData.destroy(e.message || 'Failed send #2e4uxDql');
            });
        });
        sendPromise.catch(function (){});
        return sendPromise;
    };

    /**
     *
     * @param type
     * @param data
     * @param ttl
     * @returns {Promise<xSocket.Data>}
     */
    this.sendReadyResponse = function (type, data, ttl) {
        return new Promise(function (resolve, reject){
            $this.send(type, data, ttl).then(function(xSocketData) {
                if(xSocketData.isResponse() || xSocketData.getDestroy()){
                    if(xSocketData.getError()){
                        return reject(new Error(xSocketData.getError()));
                    }
                    return resolve(xSocketData);
                }

                xSocketData.on('response', function (data, err){
                    if(err){
                        return reject(new Error(err));
                    }
                    resolve(xSocketData);
                });

            }).catch(function (e){
                reject(e);
            })
        });
    };

    /**
     *
     * * @returns {Promise<xSocket.Data>}
     */
    this.sendReady = this.sendReadyResponse;

    /**
     *
     * @param msg
     * @returns {boolean}
     */
    this.destroy = function (msg){
        if($this.isDestroy()){
            return false;
        }
        __destroy = String(msg || 'destroy');
        this.disconnect(__destroy);
        this.emit('destroy', this, __destroy);
        // Clear all events
        this.clearAllListerens();
        return true
    };

    /**
     *
     * @param msg
     * @returns {boolean}
     */
    this.disconnect = function (msg){
        var ws = this.getConnection();
        if(!ws || !this.isConnection()){
            return false;
        }
        return ws.closeConnection(msg);
    };

    /**
     *
     * @param ws
     * @returns {boolean}
     */
    this.update = function (ws){
        if(typeof ws !== 'object' || typeof ws.isConnection !== 'function' || typeof ws.query !== 'object'){
            return false;
        }
        if(!ws.isConnection() || __destroy){
            return false;
        }
        if(__ID !== ws.query['xSOId'] || __sign !== ws.query['xSOSign']){
            __ID   = ws.query['xSOId'];
            __sign = ws.query['xSOSign'];
            __query = Object(ws['query'] || {});
            if(__query['xSOId']) delete  __query['xSOId'];
            if(__query['xSOSign']) delete  __query['xSOSign'];
            $this.emit('update', $this);
        }
        if($this.isServer()){
            ws.sendObject(['SO|update', {
                'xSOId': $this.getID(),
                'xSOSign' : $this.getSign(),
                'configure' : typeof __ServerConfigure === 'object' ? __ServerConfigure : {}
            }]);
        }
        __ws = ws;
        ws.SocketObject = ws;
        $this.emit('ws|connect', ws);
        return true;
    };

    /**
     *
     * @returns {boolean}
     */
    this.isServer = function (){
        return !!__ServerConfigure;
    };

    /**
     *
     */
    (function (){

        $this.on('destroy', function (){
            var SDlist = $this.getSocketDataList();
            for(var i in SDlist){
                try{
                    SDlist[i].destroy('SocketObject|destroy');
                }catch (e){}
            }
        });
        $this.on('update', function (){
            if(!$this.isServer()){

            }
        });
        $this.on('ws|disconnect', function (msg){
            msg = typeof msg === 'string' ? msg : 'unknown';
            $this.emit('disconnect', $this, msg);
            if(msg === 'end'){
                $this.destroy('end');
            }
        });
        $this.on('ws|connect', function (){
            $this.emit('connect', $this);
            if(!__isNewConnection){
                $this.emit('reconnect', $this);
            }else{
                __isNewConnection = false;
            }
        });
        $this.on('ws|message', function (type, data){
            if(
                typeof type !== 'string' ||
                !type.length ||
                typeof data !== 'object' ||
                typeof data['ID'] !== 'string' ||
                !xSocket.helpers.checkUID(data['ID']) ||
                !data['ID'].length ||
                typeof data['data'] !== 'object'
            ){
                return;
            }
            var socketData;
            if(type === 'SD|create'){
                if(typeof data['type'] == 'string' && data['type'].length || !$this.getSocketData(data['ID'])){
                    socketData = new xSocket.Data(false, data, $this);
                    socketData.response = function (data, error){
                        var p = new Promise(function (resolve, reject){
                            if(socketData.getDestroy() || socketData.isResponse() || !socketData.getTTL()){
                                return reject('destroy');
                            }
                            socketData.setResData(data);
                            socketData.setError(error);
                            socketData.on('destroy', function (msg){
                                reject(new Error(msg || 'Error #sk2jrqF'))
                            });
                            socketData.on('response', function (){
                                resolve(socketData);
                            });
                            $this.getWsReady().then(function (ws){
                                if(ws.sendObject(['SD|update', {
                                        'ID' : socketData.getID(),
                                        'data' : socketData.getResData(),
                                        'error' : socketData.getError()
                                    }
                                ])){
                                    socketData.setStatus(xSocket.Data.STATUS_RESPONSE);
                                }else{
                                    socketData.destroy('Error #e318Js');
                                }
                            }).catch(function (e){
                                socketData.destroy(e.message || 'Error #er8avhT');
                            });
                        });
                        p.catch(function(){});
                        return p;
                    };
                    $this.emit('data', socketData);
                    $this.emit('data|'+socketData.getType(), socketData);
                    if(!socketData.getTTL()){
                        socketData.setStatus(xSocket.Data.STATUS_RESPONSE);
                    }
                }
            }else if(type === 'SD|update'){
                socketData = $this.getSocketData(data['ID']);
                if(socketData){
                    socketData.setResData(data['data']);
                    socketData.setError(data['error'] || false);
                    socketData.setStatus(xSocket.Data.STATUS_RESPONSE);
                }
            }
        });

    })();
};

xSocket.Client = function xSocketClient(serverUrl, __query, __settings){

    // Extend
    xSocket.helpers.EventEmitter.call(this);
    this.setMaxListeners(0);

    var $this = this;
    var __urlList = [];
    var __ws;
    var __SocketObject = false;


    /**
     *
     * @returns {{}}
     */
    this.getSettings = function(){
        return __settings || {};
    };

    /**
     *
     * @param key
     * @param def
     * @returns {*}
     */
    this.getSettingsVal = function (key, def){
        return this.getSettings()[key] || def;
    };

    /**
     *
     * @returns {xSocket.xSocketObject}
     */
    this.getSocketObject = function (){
        return __SocketObject;
    };


    /**
     *
     * @returns {Promise<xSocket.xSocketObject>}
     */
    this.getSocketObjectReady = function (){
        return new Promise(function (resolve) {
            if($this.getSocketObject()) return resolve($this.getSocketObject());
            $this.on('connect', function (socketObject) {
                resolve(socketObject);
            });
        });
    }

    /**
     *
     * @returns {Promise<{}>}
     */
    this.getWs = function (){
        return new Promise(function (resolve){
            if(typeof __ws === 'object'){
                return resolve(__ws);
            }
            if(__ws === null){
                return $this.once('ws|connect', function (ws){
                    resolve(ws);
                });
            }
            __ws = null;
            var url = __urlList.length > 1 ? xSocket.helpers.getRandInt(0, __urlList.length - 1) : __urlList[0];
            url += (url.indexOf('?') === -1 ? '?' : '&');

            if(__SocketObject && __SocketObject.getID().length && __SocketObject.getSign().length){
                __query['xSOId'] = String(__SocketObject.getID());
                __query['xSOSign'] = String(__SocketObject.getSign());
            }

            if(Object.keys(__query).length){
                for (var i in __query){
                    try{
                        url += encodeURIComponent(String(i))+'='+encodeURIComponent(String(__query[i]))+'&';
                    }catch (e){}
                }
            }

            var ws = new xSocket.WebSocket(url);
            ws.closeConnection = function (msg){
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
            ws.isConnection = function (){
                return ws.readyState === 1;
            };
            ws.sendMessage = function (msg){
                try{
                    if(this.isConnection()){
                        ws.send(String(msg));
                        return true;
                    }
                }catch (e){}
                return false;
            };
            ws.sendObject = function(object) {
                try {
                    if(ws.isConnection()){
                        ws.send(JSON.stringify(object));
                        return true;
                    }
                }catch (e){}
                return false;
            };
            var msg;
            var msgInc = 0;
            var pongTimeout = false;
            ws.onopen = function (){
                __ws = ws;
                $this.emit('ws|connect', ws);
                pongTimeout = setInterval(function (){
                    if(!msgInc){
                        ws.closeConnection('pingTimeout');
                    }
                    msgInc = 0;
                }, 21000);
                resolve(__ws);
            }
            ws.onmessage = function (e){
                msgInc++;
                var msg = String(e.data || '');
                var body = '';
                if(msg === 'ping'){
                    return ws.sendMessage('pong');
                }
                try {
                    body = JSON.parse(msg);
                    if (typeof body !== 'object' || typeof body[0] !== 'string' || typeof body[1] !== 'object') {
                        return;
                    }
                    var type = body[0];
                    var data = body[1];
                    if (type === 'SO|update') {
                        ws.query = data;
                        if(__SocketObject){
                            if(data['xSOId'] === __SocketObject.getID() && data['xSOSign'] === __SocketObject.getSign() && __SocketObject.update(ws)){
                                return false;
                            }
                            __SocketObject.destroy('update');
                            __SocketObject = undefined;
                        }
                        __SocketObject = new xSocket.xSocketObject();
                        __SocketObject.once('connect', function() {
                            $this.emit('connect', __SocketObject);
                        });
                        __SocketObject.on('destroy', function(socketObject, msg) {
                            __SocketObject = false;
                            $this.emit('destroy', socketObject, msg);
                        });
                        __SocketObject.on('disconnect', function() {
                            $this.emit('disconnect', __SocketObject);
                        });
                        __SocketObject.on('reconnect', function() {
                            $this.emit('reconnect', __SocketObject);
                        });

                        __SocketObject.on('data', function(socketData) {
                            $this.emit('data', socketData);
                        });
                        if(!__SocketObject.update(ws)){
                            ws.closeConnection('update');
                        }
                    }else{
                        if(__SocketObject){
                            __SocketObject.emit('ws|message', body[0], body[1]);
                        }
                    }
                }catch (e){}
            };
            ws.onclose = function (e){
                if(pongTimeout) try{ clearInterval(pongTimeout); }catch (e){}

                __ws = undefined;
                msg = msg || e.reason || 'close';
                $this.emit('ws|disconnect', msg);
                if(__SocketObject){
                    __SocketObject.emit('ws|disconnect', msg);
                }
            };
            ws.onerror = function (e){
                msg = e.message;
                $this.emit('ws|error', e.message, ws);
            };
        });
    };

    /**
     *
     * @returns {Promise<xSocket.Data>}
     */
    this.send = function (){
        var ar = arguments;
        return new Promise(function (resolve, reject){
            $this.getSocketObjectReady().then(function (){
                __SocketObject.send.apply(this, ar).then(function (e){
                    resolve(e);
                }).catch(function (e){
                    reject(e);
                });
            }).catch(function (e){
                reject(e);
            });
        });
    };

    /**
     *
     * @param type
     * @param data
     * @param ttl
     * @returns {Promise<xSocket.Data>}
     */
    this.sendReadyResponse = function (type, data, ttl){
        return new Promise(function (resolve, reject){
            $this.getSocketObjectReady().then(function (socketObject){
                socketObject.sendReadyResponse(type, data, ttl).then(function (e){
                    resolve(e);
                }).catch(function (e){
                    reject(e);
                });
            }).catch(function (e){
                reject(e);
            });
        });
    };

    /**
     *
     * @param type
     * @param data
     * @param ttl
     * @returns {Promise<xSocket.Data>}
     */
    this.sendReady = function (type, data, ttl){
        return new Promise(function (resolve, reject){
            $this.getSocketObjectReady().then(function (socketObject){
                socketObject.sendReady(type, data, ttl).then(function (e){
                    resolve(e);
                }).catch(function (e){
                    reject(e);
                });
            }).catch(function (e){
                reject(e);
            });
        });
    }

    /**
     *
     * @param msg
     * @returns {*}
     */
    this.disconnect = function (msg){
        var socketObject = this.getSocketObject();
        if(!socketObject) return false;
        return socketObject.disconnect(msg);
    };

    /**
     * Construct
     */
    (function (){
        if(typeof serverUrl === "object"){
            for(var i in serverUrl){
                __urlList.push(serverUrl[i]);
            }
        }else if(typeof serverUrl === "string"){
            __urlList.push(serverUrl);
        }
        if(!__urlList.length){
            throw new Error('Invalid urlList');
        }
        __settings = typeof __settings === 'object' ? __settings : {};
        __query     = typeof __query === 'object' ? __query : {};
        $this.on('ws|disconnect', function(){
            setTimeout(function(){
                    $this.getWs()['catch'](function (){});
            }, 1000);
        });
        $this.on('ws|error', function(){
            setTimeout(function(){
                $this.getWs()['catch'](function (){});
            }, 1000);
        });
        if(xSocket.helpers.getWindow()){
            var closeConnection = function() {
                try{
                    __ws.closeConnection('end');
                }catch (e){}
            };
            xSocket.helpers.getWindow().addEventListener("pagehide", closeConnection);
            xSocket.helpers.getWindow().addEventListener("unload", closeConnection);
        }else if(xSocket.helpers.getProcess()){
            xSocket.helpers.getProcess().on('exit', function (){
                try{
                    __ws.closeConnection('end');
                }catch (e){}
            });
        }
        $this.on('connect', function (socketObject){
            var destroyTimeout = false;
            var setDestroyTimeout = function (action, ttl){
                if(destroyTimeout){
                    try{
                        clearTimeout(destroyTimeout);
                        destroyTimeout = false;
                    }catch (e){}
                }
                if(action){
                    ttl = typeof ttl === 'number' && !isNaN(ttl) && ttl > 0 ? ttl : 0;
                    destroyTimeout = setTimeout(function (socketObject){
                        socketObject.destroy(ttl);
                        destroyTimeout = false;
                    }, ttl, socketObject);
                }
            }

            socketObject.on('reconnect', function (){
                setDestroyTimeout(false);
            });
            socketObject.on('disconnect', function (){
                var socketObjectTimeout = parseInt(Object(socketObject.getQueryVal('configure', {}))['socketObjectTimeout'] || 0);
                if(typeof socketObjectTimeout === 'number'){
                    setDestroyTimeout(true, socketObjectTimeout);
                }
            });
            socketObject.once('destroy', function (){
                setDestroyTimeout(false);
            });
        });
        $this.getWs().catch(function (){});
    })();

};
