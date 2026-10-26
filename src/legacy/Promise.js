import PromisePolyfill from 'promise-polyfill';

export default typeof Promise === 'function' && typeof Promise.prototype.then === 'function' ? Promise : PromisePolyfill;
