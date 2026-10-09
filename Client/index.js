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
