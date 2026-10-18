xSocket.Client = function xSocketClient(serverUrl, __query, __settings){

    // Extend
    xSocket.helpers.EventEmitter.call(this);
    this.setMaxListeners(0);

    var $this = this;
    var __urlList = [];
    var __ws;
    var __SocketObject = false;
    var __isVersionWarned = false;


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
            $this.once('connect', function (socketObject) {
                resolve(socketObject);
            });
        });
    };

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
            var url = __urlList[xSocket.helpers.getRandInt(0, __urlList.length - 1)];
            url += (url.indexOf('?') === -1 ? '?' : '&');

            __query['xV'] = xSocket.VERSION;
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
            ws.binaryType = 'arraybuffer';
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
                        ws.send(xSocket.helpers.encodeMessage(object));
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
                if(e.data === 'ping'){
                    return ws.sendMessage('pong');
                }
                try {
                    var body = xSocket.helpers.decodeMessage(e.data);
                    if (!body) {
                        return;
                    }
                    if (body[0] === xSocket.xSocketObject.MSG_SO_UPDATE) {
                        if(body[4] !== xSocket.VERSION && !__isVersionWarned){
                            __isVersionWarned = true;
                            console.warn('[warn|xSocket]', 'Version mismatch', 'client ' + xSocket.VERSION + ', server ' + (body[4] || 'unknown'));
                        }
                        ws.timeOffset = typeof body[5] === 'number' ? body[5] - Date.now() : 0;
                        ws.query = {
                            'xSOId' : String(body[1]),
                            'xSOSign' : String(body[2]),
                            'configure' : typeof body[3] === 'object' ? body[3] : {}
                        };
                        if(__SocketObject){
                            if(ws.query['xSOId'] === __SocketObject.getID() && ws.query['xSOSign'] === __SocketObject.getSign() && __SocketObject.update(ws)){
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
                        __SocketObject.on('disconnect', function(socketObject, msg) {
                            $this.emit('disconnect', socketObject, msg);
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
                            __SocketObject.emit('ws|message', body);
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
     * @param type
     * @param data
     * @param ttl
     * @returns {Promise<xSocket.Data>}
     */
    this.send = function (type, data, ttl){
        return $this.getSocketObjectReady().then(function (socketObject){
            return socketObject.send(type, data, ttl);
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
        return $this.getSocketObjectReady().then(function (socketObject){
            return socketObject.sendReady(type, data, ttl);
        });
    };

    /**
     *
     * @returns {Promise<xSocket.Data>}
     */
    this.sendReadyResponse = this.sendReady;

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
                        socketObject.destroy('socketObjectTimeout');
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
xSocket.Client.prototype = Object.create(xSocket.helpers.EventEmitter.prototype);
xSocket.Client.prototype.constructor = xSocket.Client;
