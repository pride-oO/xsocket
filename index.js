/**
 * xSocket
 * @licence MIT
 * @author Vadim Pride (pride.mk.ua@gmail.com)
 * @link https://github.com/VadimPride
 */

require('./Helpers/nodejs');
require('./Helpers');
require('./Data');
require('./Object');
require('./Client');
require('./Server');

module.exports = global.xSocket;
module.exports.Server = global.xSocket.Server;
module.exports.Client = global.xSocket.Client;
