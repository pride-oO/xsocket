import xSocket from './xSocket.js';
import EventEmitter from './EventEmitter.js';

export const MSG_SO_UPDATE = 0;
export const MSG_SD_CREATE = 1;
export const MSG_SD_UPDATE = 2;
export const MSG_SO_TIME   = 3;

/**
 *
 * @returns {Window|*}
 */
export const getWindow = () => xSocket.window;

/**
 *
 * @returns {any}
 */
export const getProcess = () => xSocket.process;

/**
 *
 * @param min
 * @param max
 * @returns {number}
 */
export const getRandInt = (min, max) => Math.floor(min + Math.random() * (max + 1 - min));

/**
 *
 * @param data
 * @returns {boolean}
 */
export const isBinary = (data) => typeof ArrayBuffer === 'function' && (data instanceof ArrayBuffer || ArrayBuffer.isView(data));

/**
 *
 * @param type
 * @returns {number}
 */
export const getMessageDataIndex = (type) => type === MSG_SD_CREATE ? 4 : 2;

/**
 *
 * @param text
 * @returns {Uint8Array}
 */
export const encodeText = (text) => {
    if(typeof TextEncoder === 'function'){
        return new TextEncoder().encode(text);
    }
    const binary = unescape(encodeURIComponent(text));
    const byteList = new Uint8Array(binary.length);
    for(let i = 0; i < binary.length; i++){
        byteList[i] = binary.charCodeAt(i);
    }
    return byteList;
};

/**
 *
 * @param byteList
 * @returns {string}
 */
export const decodeText = (byteList) => {
    if(typeof TextDecoder === 'function'){
        return new TextDecoder().decode(byteList);
    }
    let binary = '';
    for(let i = 0; i < byteList.length; i++){
        binary += String.fromCharCode(byteList[i]);
    }
    return decodeURIComponent(escape(binary));
};

/**
 *
 * @param body
 * @returns {string|Uint8Array}
 */
export const encodeMessage = (body) => {
    const dataIndex = getMessageDataIndex(body[0]);
    const data = body[dataIndex];
    if(!isBinary(data)){
        return JSON.stringify(body);
    }
    body = body.slice();
    body[dataIndex] = null;
    const headerBytes = encodeText(JSON.stringify(body));
    const dataBytes = data instanceof ArrayBuffer ? new Uint8Array(data) : new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    const frame = new Uint8Array(4 + headerBytes.length + dataBytes.length);
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
export const decodeMessage = (raw) => {
    let body;
    try{
        if(typeof raw === 'string'){
            body = JSON.parse(raw);
        }else{
            const frame = raw instanceof ArrayBuffer ? new Uint8Array(raw) : raw;
            const headerLength = new DataView(frame.buffer, frame.byteOffset, frame.byteLength).getUint32(0);
            body = JSON.parse(decodeText(frame.subarray(4, 4 + headerLength)));
            if(Array.isArray(body)){
                body[getMessageDataIndex(body[0])] = frame.subarray(4 + headerLength);
            }
        }
    }catch (e){
        return false;
    }
    return Array.isArray(body) && typeof body[0] === 'number' ? body : false;
};

export default {
    getWindow,
    getProcess,
    getRandInt,
    isBinary,
    getMessageDataIndex,
    encodeText,
    decodeText,
    encodeMessage,
    decodeMessage,
    EventEmitter
};
