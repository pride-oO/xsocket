export default class EventEmitter {

    constructor(){
        this.__listenerMap = Object.create(null);
        this.__maxListeners = 10;
    }

    /**
     * Append EE listener
     * @param name
     * @param callback
     * @param once
     * @return {EventEmitter}
     */
    on(name, callback, once){
        if(typeof name !== 'string' || typeof callback !== 'function'){
            return this;
        }
        const listenerList = this.__listenerMap[name] || (this.__listenerMap[name] = []);
        listenerList.push({'callback' : callback, 'once' : once === true});
        if(this.__maxListeners > 0 && listenerList.length > this.__maxListeners){
            console.warn('EventEmitter ', name, ', exceeded maxListeners : ', this.__maxListeners + '/' + listenerList.length, '. Use EE.setMaxListeners(n)!');
        }
        return this;
    }

    /**
     * Append once EE listener
     * @param name
     * @param callback
     * @return {EventEmitter}
     */
    once(name, callback){
        return this.on(name, callback, true);
    }

    /**
     * Getter all listener name
     * @returns {string[]}
     */
    getListerensName(){
        return Object.keys(this.__listenerMap);
    }

    /**
     * Clear all listener
     * @returns {{}}
     */
    clearAllListerens(){
        this.__listenerMap = Object.create(null);
        return this.__listenerMap;
    }

    /**
     * Getter count EE listener
     * @param name
     * @returns {number}
     */
    listenerCount(name){
        const listenerList = this.__listenerMap[name];
        return listenerList ? listenerList.length : 0;
    }

    /**
     * Set max listeners
     * @param count
     * @returns {boolean}
     */
    setMaxListeners(count){
        if(typeof count !== 'number'){
            return false;
        }
        this.__maxListeners = parseInt(count);
        return true;
    }

    /**
     * Delete EE listener
     * @param name
     * @param callback
     * @return {EventEmitter}
     */
    off(name, callback){
        const listenerList = this.__listenerMap[name];
        if(!listenerList || typeof callback !== 'function'){
            return this;
        }
        for(let i = listenerList.length - 1; i >= 0; i--){
            if(listenerList[i].callback === callback){
                listenerList.splice(i, 1);
            }
        }
        if(!listenerList.length){
            delete this.__listenerMap[name];
        }
        return this;
    }

    /**
     * Delete all EE listener
     * @param name
     * @return {EventEmitter}
     */
    offAll(name){
        delete this.__listenerMap[name];
        return this;
    }

    /**
     * Emit EE listener
     * @param name
     * @param argList
     * @return {EventEmitter}
     */
    emit(name, ...argList){
        let listenerList = this.__listenerMap[name];
        if(!listenerList){
            return this;
        }
        listenerList = listenerList.slice();
        for(let i = 0; i < listenerList.length; i++){
            const currentList = this.__listenerMap[name];
            const index = currentList ? currentList.indexOf(listenerList[i]) : -1;
            if(index === -1){
                continue;
            }
            if(listenerList[i].once){
                currentList.splice(index, 1);
                if(!currentList.length){
                    delete this.__listenerMap[name];
                }
            }
            listenerList[i].callback.apply(this, argList);
        }
        return this;
    }
}
