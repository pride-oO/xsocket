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
                return reject(new Error($this.getDestroy()));
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
