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
    };

    /**
     *
     * @param data
     * @returns {boolean}
     */
    var isBinary = this.isBinary = function (data){
        return typeof ArrayBuffer === 'function' && (data instanceof ArrayBuffer || ArrayBuffer.isView(data));
    };

    /**
     *
     * @param type
     * @returns {number}
     */
    var getMessageDataIndex = this.getMessageDataIndex = function (type){
        return type === xSocket.xSocketObject.MSG_SD_CREATE ? 4 : 2;
    };

    /**
     *
     * @param body
     * @returns {string|Uint8Array}
     */
    this.encodeMessage = function (body){
        var dataIndex = getMessageDataIndex(body[0]);
        var data = body[dataIndex];
        if(!isBinary(data)){
            return JSON.stringify(body);
        }
        body = body.slice();
        body[dataIndex] = null;
        var headerBytes = new TextEncoder().encode(JSON.stringify(body));
        var dataBytes = data instanceof ArrayBuffer ? new Uint8Array(data) : new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
        var frame = new Uint8Array(4 + headerBytes.length + dataBytes.length);
        new DataView(frame.buffer).setUint32(0, headerBytes.length);
        frame.set(headerBytes, 4);
        frame.set(dataBytes, 4 + headerBytes.length);
        return frame;
    };

    /**
     *
     * @param raw
     * @returns {Array|boolean}
     */
    this.decodeMessage = function (raw){
        var body;
        try{
            if(typeof raw === 'string'){
                body = JSON.parse(raw);
            }else{
                var frame = raw instanceof ArrayBuffer ? new Uint8Array(raw) : raw;
                var headerLength = new DataView(frame.buffer, frame.byteOffset, frame.byteLength).getUint32(0);
                body = JSON.parse(new TextDecoder().decode(frame.subarray(4, 4 + headerLength)));
                if(Array.isArray(body)){
                    body[getMessageDataIndex(body[0])] = frame.subarray(4 + headerLength);
                }
            }
        }catch (e){
            return false;
        }
        return Array.isArray(body) && typeof body[0] === 'number' ? body : false;
    };

    /**
     *
     * @constructor
     */
    var EventEmitter = this.EventEmitter = function EventEmitter(){
        this.__listenerMap = Object.create(null);
        this.__maxListeners = 10;
    };

    /**
     * Append EE listener
     * @param name
     * @param callback
     * @param once
     * @return {EventEmitter}
     */
    EventEmitter.prototype.on = function (name, callback, once){
        if(typeof name !== 'string' || typeof callback !== 'function'){
            return this;
        }
        var listenerList = this.__listenerMap[name] || (this.__listenerMap[name] = []);
        listenerList.push({'callback' : callback, 'once' : once === true});
        if(this.__maxListeners > 0 && listenerList.length > this.__maxListeners){
            console.warn('EventEmitter ', name, ', exceeded maxListeners : ', this.__maxListeners + '/' + listenerList.length, '. Use EE.setMaxListeners(n)!');
        }
        return this;
    };

    /**
     * Append once EE listener
     * @param name
     * @param callback
     * @return {EventEmitter}
     */
    EventEmitter.prototype.once = function (name, callback){
        return this.on(name, callback, true);
    };

    /**
     * Getter all listener name
     * @returns {string[]}
     */
    EventEmitter.prototype.getListerensName = function (){
        return Object.keys(this.__listenerMap);
    };

    /**
     * Clear all listener
     * @returns {{}}
     */
    EventEmitter.prototype.clearAllListerens = function (){
        this.__listenerMap = Object.create(null);
        return this.__listenerMap;
    };

    /**
     * Getter count EE listener
     * @param name
     * @returns {number}
     */
    EventEmitter.prototype.listenerCount = function (name){
        var listenerList = this.__listenerMap[name];
        return listenerList ? listenerList.length : 0;
    };

    /**
     * Set max listeners
     * @param count
     * @returns {boolean}
     */
    EventEmitter.prototype.setMaxListeners = function (count){
        if(typeof count !== 'number'){
            return false;
        }
        this.__maxListeners = parseInt(count);
        return true;
    };

    /**
     * Delete EE listener
     * @param name
     * @param callback
     * @return {EventEmitter}
     */
    EventEmitter.prototype.off = function (name, callback){
        var listenerList = this.__listenerMap[name];
        if(!listenerList || typeof callback !== 'function'){
            return this;
        }
        for(var i = listenerList.length - 1; i >= 0; i--){
            if(listenerList[i].callback === callback){
                listenerList.splice(i, 1);
            }
        }
        if(!listenerList.length){
            delete this.__listenerMap[name];
        }
        return this;
    };

    /**
     * Delete all EE listener
     * @param name
     * @return {EventEmitter}
     */
    EventEmitter.prototype.offAll = function (name){
        delete this.__listenerMap[name];
        return this;
    };

    /**
     * Emit EE listener
     * @param name
     * @param data_1
     * @param data_2
     * @param data_3
     * @param data_4
     * @param data_5
     * @return {EventEmitter}
     */
    EventEmitter.prototype.emit = function (name, data_1, data_2, data_3, data_4, data_5){
        var listenerList = this.__listenerMap[name];
        if(!listenerList){
            return this;
        }
        listenerList = listenerList.slice();
        for(var i = 0; i < listenerList.length; i++){
            var currentList = this.__listenerMap[name];
            var index = currentList ? currentList.indexOf(listenerList[i]) : -1;
            if(index === -1){
                continue;
            }
            if(listenerList[i].once){
                currentList.splice(index, 1);
                if(!currentList.length){
                    delete this.__listenerMap[name];
                }
            }
            listenerList[i].callback.call(this, data_1, data_2, data_3, data_4, data_5);
        }
        return this;
    };
};
xSocket.VERSION = '2.1.0';
