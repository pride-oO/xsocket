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
        ws.SocketObject = $this;
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
                if(typeof data['type'] == 'string' && data['type'].length && !$this.getSocketData(data['ID'])){
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
