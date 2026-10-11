/**
 *
 * @param __ServerConfigure
 * @param __req
 * @constructor
 */
xSocket.xSocketObject = function xSocketObject(__ServerConfigure, __req){
    xSocket.helpers.EventEmitter.call(this);
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

    this.on('destroy', this.__onDestroy);
    this.on('ws|disconnect', this.__onWsDisconnect);
    this.on('ws|connect', this.__onWsConnect);
    this.on('ws|message', this.__onWsMessage);
};
xSocket.xSocketObject.prototype = Object.create(xSocket.helpers.EventEmitter.prototype);
xSocket.xSocketObject.prototype.constructor = xSocket.xSocketObject;
xSocket.xSocketObject.MSG_SO_UPDATE = 0;
xSocket.xSocketObject.MSG_SD_CREATE = 1;
xSocket.xSocketObject.MSG_SD_UPDATE = 2;

/**
 *
 * @returns {string}
 */
xSocket.xSocketObject.prototype.getID = function (){
    return this.__ID || '';
};

/**
 *
 * @returns {string}
 */
xSocket.xSocketObject.prototype.getSign = function (){
    return this.__sign || '';
};

/**
 *
 * @returns {*|{}}
 */
xSocket.xSocketObject.prototype.getReq = function (){
    return this.__req || {};
};

/**
 *
 * @returns {*}
 */
xSocket.xSocketObject.prototype.getQuery = function (){
    return typeof this.__query === 'object' ? this.__query : {};
};

/**
 *
 * @param key
 * @param def
 * @returns {*}
 */
xSocket.xSocketObject.prototype.getQueryVal = function (key, def){
    return this.getQuery()[key] || def;
};

/**
 *
 * @returns {*|{}}
 */
xSocket.xSocketObject.prototype.getReqHeaders = function (){
    return this.getReq()['headers'] || {};
};

/**
 *
 * @returns {*|string}
 */
xSocket.xSocketObject.prototype.getOrigin = function (){
    return this.getReqHeaders()['origin'] || 'unknown';
};

/**
 *
 * @returns {string}
 */
xSocket.xSocketObject.prototype.getIP = function (){
    return String((this.getReq()['connection'] || {})['remoteAddress'] || 'unknown');
};

/**
 *
 * @returns {{}}
 */
xSocket.xSocketObject.prototype.getSocketDataList = function (){
    return this.__socketDataMap;
};

/**
 *
 * @param ID
 * @returns {xSocket.Data|boolean}
 */
xSocket.xSocketObject.prototype.getSocketData = function (ID){
    return this.__socketDataMap[ID] || false;
};

/**
 *
 * @returns {boolean}
 */
xSocket.xSocketObject.prototype.isConnection = function (){
    return typeof this.__ws === 'object' ? this.__ws.isConnection() : false;
};

/**
 *
 * @returns {any|boolean}
 */
xSocket.xSocketObject.prototype.getConnection = function (){
    return this.isConnection() ? this.__ws : false;
};

/**
 *
 * @returns {string|boolean}
 */
xSocket.xSocketObject.prototype.getDestroy = function (){
    return this.__destroy || false;
};

/**
 *
 * @returns {boolean}
 */
xSocket.xSocketObject.prototype.isDestroy = function (){
    return !!this.getDestroy();
};

/**
 *
 * @returns {boolean}
 */
xSocket.xSocketObject.prototype.isServer = function (){
    return !!this.__serverConfigure;
};

/**
 *
 * @returns {Promise}
 */
xSocket.xSocketObject.prototype.getWsReady = function (){
    var $this = this;
    return new Promise(function (resolve, reject){
        if($this.getConnection()){
            return resolve($this.getConnection());
        }
        var destroyHandler = function (){
            $this.off('connect', connectHandler);
            reject(new Error('destroy'));
        };
        var connectHandler = function (){
            $this.off('destroy', destroyHandler);
            if($this.getConnection()){
                resolve($this.getConnection());
            }else{
                reject(new Error('Connection failed'));
            }
        };
        $this.once('connect', connectHandler);
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
xSocket.xSocketObject.prototype.send = function (type, data, ttl){
    var $this = this;
    var socketData = new xSocket.Data(true, {
        'ID' : String(++this.__socketDataInc),
        'type' : type,
        'data' : data,
        'ttl' : ttl
    }, this);
    if(socketData.getTTL()){
        this.__socketDataMap[socketData.getID()] = socketData;
        socketData.once('destroy', function (){
            delete $this.__socketDataMap[socketData.getID()];
        });
    }
    this.getWsReady().then(function (ws){
        if(!ws.sendObject([
            xSocket.xSocketObject.MSG_SD_CREATE,
            Number(socketData.getID()),
            socketData.getType(),
            socketData.getTTL(),
            socketData.getData()
        ])){
            return socketData.destroy('Send failed');
        }
        socketData.setStatus(socketData.getTTL() ? xSocket.Data.STATUS_SEND : xSocket.Data.STATUS_RESPONSE);
    }).catch(function (e){
        socketData.destroy(e.message || 'Send failed');
    });
    return Promise.resolve(socketData);
};

/**
 *
 * @param type
 * @param data
 * @param ttl
 * @returns {Promise<xSocket.Data>}
 */
xSocket.xSocketObject.prototype.sendReadyResponse = function (type, data, ttl){
    return this.send(type, data, ttl).then(function (socketData){
        return new Promise(function (resolve, reject){
            var responseHandler = function (){
                if(socketData.getError()){
                    return reject(new Error(socketData.getError()));
                }
                resolve(socketData);
            };
            if(socketData.isResponse() || socketData.getDestroy()){
                return responseHandler();
            }
            socketData.once('response', responseHandler);
        });
    });
};

/**
 *
 * @returns {Promise<xSocket.Data>}
 */
xSocket.xSocketObject.prototype.sendReady = xSocket.xSocketObject.prototype.sendReadyResponse;

/**
 *
 * @param msg
 * @returns {boolean}
 */
xSocket.xSocketObject.prototype.destroy = function (msg){
    if(this.isDestroy()){
        return false;
    }
    this.__destroy = String(msg || 'destroy');
    this.disconnect(this.__destroy);
    this.emit('destroy', this, this.__destroy);
    this.clearAllListerens();
    return true;
};

/**
 *
 * @param msg
 * @returns {boolean}
 */
xSocket.xSocketObject.prototype.disconnect = function (msg){
    var ws = this.getConnection();
    if(!ws){
        return false;
    }
    return ws.closeConnection(msg);
};

/**
 *
 * @param ws
 * @returns {boolean}
 */
xSocket.xSocketObject.prototype.update = function (ws){
    if(typeof ws !== 'object' || typeof ws.isConnection !== 'function' || typeof ws.query !== 'object'){
        return false;
    }
    if(!ws.isConnection() || this.__destroy){
        return false;
    }
    if(this.__ID !== ws.query['xSOId'] || this.__sign !== ws.query['xSOSign']){
        this.__ID   = ws.query['xSOId'];
        this.__sign = ws.query['xSOSign'];
        this.__query = Object(ws['query'] || {});
        delete this.__query['xSOId'];
        delete this.__query['xSOSign'];
        delete this.__query['xV'];
        this.emit('update', this);
    }
    if(this.isServer()){
        ws.sendObject([
            xSocket.xSocketObject.MSG_SO_UPDATE,
            this.getID(),
            this.getSign(),
            typeof this.__serverConfigure === 'object' ? this.__serverConfigure : {},
            xSocket.VERSION
        ]);
    }
    this.__ws = ws;
    ws.SocketObject = this;
    this.emit('ws|connect', ws);
    return true;
};

xSocket.xSocketObject.prototype.__onDestroy = function (){
    for(var ID in this.__socketDataMap){
        this.__socketDataMap[ID].destroy('SocketObject|destroy');
    }
};

xSocket.xSocketObject.prototype.__onWsDisconnect = function (msg){
    msg = typeof msg === 'string' ? msg : 'unknown';
    this.emit('disconnect', this, msg);
    if(msg === 'end'){
        this.destroy('end');
    }
};

xSocket.xSocketObject.prototype.__onWsConnect = function (){
    this.emit('connect', this);
    if(!this.__isNewConnection){
        this.emit('reconnect', this);
    }else{
        this.__isNewConnection = false;
    }
};

xSocket.xSocketObject.prototype.__onWsMessage = function (body){
    var ID = body[1];
    if(typeof ID !== 'number' || ID % 1 !== 0 || ID < 1){
        return;
    }
    var socketData;
    if(body[0] === xSocket.xSocketObject.MSG_SD_CREATE){
        if(typeof body[2] !== 'string' || !body[2].length || typeof body[4] !== 'object'){
            return;
        }
        socketData = new xSocket.Data(false, {
            'ID' : String(ID),
            'type' : body[2],
            'ttl' : body[3],
            'data' : body[4]
        }, this);
        this.emit('data', socketData);
        this.emit('data|' + socketData.getType(), socketData);
        if(!socketData.getTTL()){
            socketData.setStatus(xSocket.Data.STATUS_RESPONSE);
        }
    }else if(body[0] === xSocket.xSocketObject.MSG_SD_UPDATE){
        socketData = this.getSocketData(String(ID));
        if(socketData){
            socketData.setResData(body[2]);
            socketData.setError(body[3] || false);
            socketData.setStatus(xSocket.Data.STATUS_RESPONSE);
        }
    }
};
