/*
 * Thinknode Provider
 */

var net = require('net');
var os = require('os');
var util = require('util');

var async = require('async');
var int64 = require('node-int64');
var msgpack = require('msgpack5')({
    "forceFloat64": true
});
var ErrorEngine = require('error-engine');

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// Custom Date Serialization/Deserialization

msgpack.register(0x01, Date, function(obj) {
    var buf;

    // Encode value
    var value = obj.valueOf();
    if (-128 <= value && value <= 127) {
        buf = new Buffer(3);
        buf[0] = 0xd4;
        buf[1] = 0x01;
        buf.writeInt8(value, 2);
    } else if (-32768 <= value && value <= 32767) {
        buf = new Buffer(4);
        buf[0] = 0xd5;
        buf[1] = 0x01;
        buf.writeInt16BE(value, 2);
    } else if (-2147483648 <= value && value <= 2147483647) {
        buf = new Buffer(6);
        buf[0] = 0xd6;
        buf[1] = 0x01;
        buf.writeInt32BE(value, 2);
    } else { // if (value <= -2147483649 || 2147483648 <= value) {
        buf = new Buffer(10);
        buf[0] = 0xd7;
        buf[1] = 0x01;
        (new int64(value)).toBuffer().copy(buf, 2);
    }
    return buf;
}, function(data) {
    if (data.length === 1) {
        return new Date(data.readInt8(0));
    } else if (data.length === 2) {
        return new Date(data.readInt16BE(0));
    } else if (data.length === 4) {
        return new Date(data.readInt32BE(0));
    } else {
        return new Date((new int64(data)).toNumber());
    }
});

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// Environment variables

var host = process.env.THINKNODE_HOST;
var port = parseInt(process.env.THINKNODE_PORT);
var pid = process.env.THINKNODE_PID;

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// Local variables

var action = {
    "REGISTER": 0,
    "FUNCTION": 1,
    "PROGRESS": 2,
    "RESULT": 3,
    "FAILURE": 4,
    "PING": 5,
    "PONG": 6
};

var VERSION = 0;

var PROTOCOL = new Buffer('0000', 'hex');

var templates = {
    "function_not_found": "Function not found (<%= name %>)",
    "invalid_ipc_code": "Invalid IPC message code (<%= code %>)",
    "invalid_ipc_version": "Invalid IPC version (<%= version %>)",
    "invalid_ipc_reserved": "Invalid IPC reserved byte value (<%= value %>)",
    "unsupported_ipc_code": "Unsupported IPC message code (<%= code %>)"
};

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// Local functions

function getHeader(code, length) {
    var header = new Buffer(8);
    header.writeUInt8(VERSION, 0); // Version
    header.writeUInt8(0, 1); // Reserved
    header.writeUInt8(code, 2); // Code
    header.writeUInt8(0, 3); // Reserved
    header.writeUInt32BE(length, 4); // Length
    return header;
}

function readString(buffer, offset, length) {
    return buffer.toString('utf8', offset, offset + length);
}

function readUInt8(buffer, offset, length) {
    return buffer.readUInt8(offset);
}

function readUInt16(buffer, offset, length) {
    return buffer.readUInt16BE(offset);
}

function readUInt32(buffer, offset, length) {
    return buffer.readUInt32BE(offset);
}

function readBuffer(buffer, offset, length) {
    return buffer.slice(offset, offset + length);
}

/**
 * @summary An error class for handling errors in the provider.
 *
 * @constructor
 * @param {string} type - The error type.
 * @param {object} obj - The object to use for templating replacement.
 */
function ProviderError(type, obj) {
    ErrorEngine.call(this, {
        "auto_template": true
    }, templates, type, obj);

    this.code = this.type = type;
    this.name = "ProviderError";
    Error.captureStackTrace(this, ProviderError);
}
util.inherits(ProviderError, ErrorEngine);

/**
 * @summary An error class for handling errors from the app itself.
 *
 * @constructor
 * @param {string} code - The error type.
 * @param {string} message - The error message.
 */
function AppError(code, message) {
    this.code = this.type = code;
    this.message = message;
    this.name = "AppError";
    Error.captureStackTrace(this, AppError);
}
util.inherits(AppError, Error);

/**
 * @summary Represents a calculation provider.
 * @description
 * The provider handles IPC between itself and the calculation supervisor. A provider will create a
 * socket connection with the supervisor, register itself as a provider, and begin listening for
 * messages.
 *
 * @constructor
 * @param {object} app - A dictionary mapping function names to functions.
 */
function Provider(app) {
    this.app = app;

    this.functionQueue = async.queue(this._processFunction.bind(this), 1);
    this.pingQueue = async.queue(this._processPing.bind(this), 1);
    this.messageQueue = async.queue(this._processMessage.bind(this), 1);
    this.socket = null;

    // State
    this._buffers = [];
    this._offset = 0;
    this._length = 0;
    this._mode = 0; // 0: read header, 1: read body

    // Message info
    this._bodyLength = null;
    this._version = null;
    this._code = null;
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// Public instance methods

/**
 * @summary Starts the provider.
 * @description
 * This is the only public function of the provider. It is responsible for making a socket
 * connection and registering itself as a provider using the environment variables for the host,
 * port, and pid.
 */
Provider.prototype.start = function() {
    this._connect();
    this._register();
};

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// Private control-related instance methods

/**
 * @private
 * @summary Connects to the supervisor at the given address.
 */
Provider.prototype._connect = function() {
    process.stdout.write("Connecting...");
    this.socket = net.connect(port, host);
    this.socket.on('data', this._onData.bind(this));
    console.log("done");
};

/**
 * @private
 * @summary Registers itself as a provider using the given pid.
 */
Provider.prototype._register = function() {
    process.stdout.write("Register...");
    var header = getHeader(action.REGISTER, 34);
    this.socket.write(header);
    this.socket.write(PROTOCOL);
    this.socket.write(pid);
    console.log("done");
};

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// Private IPC-related instance methods
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

/**
 * @private
 * @summary Consumes a specified number of bytes from the internal buffer.
 * @description
 * This method is responsible for consuming the specified number of bytes from the internal 
 * buffer. Note that this method does not perform any checks to ensure that there is enough data
 * present in the internal buffer and as such, this must be performed prior to calling this
 * function.
 *
 * @param {function} fcn - A function used to consume the data. This function should take three
 *   arguments: the buffer, the offset in the buffer, and the number of bytes to read.
 * @param {number} length - The number of bytes to consume.
 */
Provider.prototype._consume = function(fcn, length) {
    var value;

    // Decrement buffered length by consumption amount
    this._length -= length;

    if (length === 0) {
        return null;
    }

    // Handle whether or not first buffer has enough bytes
    if (this._offset + length <= this._buffers[0].length) {

        // Consume value from first buffer
        value = fcn(this._buffers[0], this._offset, length);
        this._offset += length;

        // Consumed entire first buffer, remove it from the buffers array
        if (this._offset === this._buffers[0].length) {
            this._offset = 0;
            this._buffers.shift();
        }

        // Return value
        return value;
    } else {

        // Value must span multiple buffers, create temporary buffer
        var buf = new Buffer(length);
        var buf_pos = 0;

        // Process buffers while there are remaining bytes
        var rem = length;
        while (rem > 0) {

            // Determine number of bytes that can be processed from first buffer
            var len = Math.min(rem, this._buffers[0].length - this._offset);

            // Copy the available bytes from the first buffer and increment the indices
            this._buffers[0].copy(buf, buf_pos, this._offset, this._offset + len);
            buf_pos += len;
            rem -= len;

            // Move to the next buffer or set the offset
            if (len === (this._buffers[0].length - this._offset)) {
                this._offset = 0;
                this._buffers.shift();
            } else {
                this._offset += len;
            }
        }

        // Parse value from temporary buffer and return
        value = fcn(buf, 0, length);
        return value;
    }
};

/**
 * @private
 * @summary Handles a Provider error.
 *
 * @param {string} type - The error type.
 * @param {string} key - The key to replace in the message string.
 * @param {string} value - The value to replace the key with in the message string.
 * @returns {boolean} The value false.
 */
Provider.prototype._critical = function(type, key, value) {
    var obj = {};
    obj[key] = value;
    return new ProviderError(type, obj);
};

/**
 * @private
 * @summary An event listener for the 'data' event.
 *
 * @param {Buffer} data - The data emitted.
 */
Provider.prototype._onData = function(data) {
    this._buffers.push(data);
    this._length += data.length;
    this._read();
};

/**
 * @private
 * @summary Reads data received over the socket, dispatching processing to the proper methods.
 */
Provider.prototype._read = function() {
    var prev = 0;
    var processed = false;
    while (this._length > 0 && (processed === true || prev !== this._length)) {
        prev = this._length;
        processed = false;

        // Read data from buffers
        if (this._mode === 0) {
            processed = this._readHeader();
        } else { // if (this._mode === 1)
            processed = this._readBody();
        }

    }
};

/**
 * @private
 * @summary Reads message body data.
 */
Provider.prototype._readBody = function() {
    if (this._length < this._bodyLength) {
        return false;
    }

    var buf = this._consume(readBuffer, this._bodyLength);
    if (this._code === action.FUNCTION) {
        this.functionQueue.push(buf, this._finishFunction.bind(this));
    } else if (this._code === action.PING) {
        this.pingQueue.push(buf, this._finishPing.bind(this));
    } else {
        throw this._critical("unsupported_ipc_code", "code", this._code);
    }

    this._mode = 0;
    return true;
};

/**
 * @private
 * @summary Reads message header data.
 */
Provider.prototype._readHeader = function() {
    if (this._length < 8) {
        return false;
    }

    // Parse and validate version
    this._version = this._consume(readUInt8, 1);
    if (this._version !== 0) {
        throw this._critical("invalid_ipc_version", "version", this._version);
    }

    // Parse first reserved byte
    var reserved = this._consume(readUInt8, 1);
    if (reserved !== 0) {
        throw this._critical("invalid_ipc_reserved", "value", reserved);
    }

    // Parse message code
    this._code = this._consume(readUInt8, 1);
    if (this._code > 6) {
        throw this._critical("invalid_ipc_code", "code", this._code);
    }

    // Parse second reserved byte
    reserved = this._consume(readUInt8, 1);
    if (reserved !== 0) {
        throw this._critical("invalid_ipc_reserved", "value", reserved);
    }

    // Parse body length
    this._bodyLength = this._consume(readUInt32, 4);

    // Assign state and return true
    this._mode = 1;
    return true;
};

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// Private message-related instance methods
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

/**
 * @private
 * @summary The callback that is called to finish a function.
 *
 * @param {Error} err - An error object or null if no error was encountered.
 * @param {*} data - The result of the function.
 */
Provider.prototype._finishFunction = function(err, data) {
    console.log('Finishing function request...');
    if (err) {
        if (err instanceof ProviderError || err instanceof AppError) {
            this._handleFailure(err.code, err.message);
        } else if (err instanceof Error) {
            this._handleFailure(err.name, err.message);
        } else {
            this._handleFailure("unhandled_error")
        }
    } else {
        this._handleResult(data);
    }
};

/**
 * @private
 * @summary The callback that is called to finish a ping/pong interchange.
 */
Provider.prototype._finishPing = function() {
    console.log('Ping finished processing...');
};

/**
 * @private
 * @summary Returns a function used for reporting failures.
 *
 * @param {function} callback - The callback to call once the failure has been handled.
 * @returns {function} The failure handler.
 */
Provider.prototype._getFailureHandler = function(callback) {
    var self = this;
    return function(code, message) {
        if (!callback.called) {
            callback.called = true;
            callback(new AppError(code, message));
        }
    };
};

/**
 * @private
 * @summary Handles failures reported by the app.
 *
 * @param {string} code - A failure code as a string.
 * @param {string} message - An info message relating to the progress of the calculation.
 */
Provider.prototype._handleFailure = function(code, message) {
    console.log('Handling calculation failure...');
    var codeLength = Buffer.byteLength(code, "utf8");
    var messageLength = Buffer.byteLength(message, "utf8");

    var length = 1 + codeLength + 2 + messageLength;
    var header = getHeader(action.FAILURE, length);

    var body = new Buffer(length);
    body.writeUInt8(codeLength, 0);
    body.write(code, 1, codeLength);
    body.writeUInt16BE(messageLength, 1 + codeLength);
    body.write(message, 1 + codeLength + 2);
    this.messageQueue.push(Buffer.concat([header, body]));
};

/**
 * @private
 * @summary Handles a progress message.
 *
 * @param {number} progress - A floating point value between 0 and 1 representing the progress of
 *   the calculation.
 * @param {string} [message] - An info message relating to the progress of the calculation
 *   (optional).
 */
Provider.prototype._handleProgress = function(progress, message) {
    console.log('Handling calculation progress...');
    message = typeof message !== "string" ? "" : message;
    var messageLength = Buffer.byteLength(message);
    var header = getHeader(action.PROGRESS, 4 + 2 + messageLength);

    var prog = new Buffer(4 + 2 + messageLength);
    prog.writeFloatBE(progress, 0);
    prog.writeUInt16BE(messageLength, 4);
    prog.write(message, 6, messageLength);
    this.messageQueue.push(Buffer.concat([header, prog]));
};

/**
 * @private
 * @summary Handles the result computed by the app.
 *
 * @param {*} result - The result to encode and report to the supervisor.
 */
Provider.prototype._handleResult = function(result) {
    console.log('Handling calculation result...');
    var encoded = msgpack.encode(result);
    var length = encoded.length;

    var header = getHeader(action.RESULT, length);
    this.messageQueue.push(Buffer.concat([header, encoded]));
};

/**
 * @private
 * @summary Processes function messages.
 *
 * @param {Buffer} data - The function message.
 * @param {function} callback - The callback to call once the function is completed or an error has
 *   been encountered.
 */
Provider.prototype._processFunction = function(data, callback) {
    console.log("Handling function...");
    var offset = 0;

    // Read name length
    var nameLength = readUInt8(data, offset, 1);
    offset += 1;

    // Read name
    var name = readString(data, offset, nameLength);
    if (typeof this.app[name] !== 'function') {
        var e = this._critical("function_not_found", "name", name);
        if (!callback.called) {
            callback.called = true;
            callback(e);
        }
    }
    offset += nameLength;

    // Read argument count
    var argCount = readUInt16(data, offset, 2);
    offset += 2;

    // Read each argument in turn
    var argLength, arg, i, args = [];
    for (i = 0; i < argCount; ++i) {
        // Read argument length
        argLength = readUInt32(data, offset, 4);
        offset += 4;

        // Read argument and unpack to JSON
        arg = readBuffer(data, offset, argLength);
        args.push(msgpack.decode(arg));
        offset += argLength;
    }

    args.push(this._handleProgress.bind(this));
    args.push(this._getFailureHandler(callback));

    try {
        var result = this.app[name].apply(null, args);
        if (!callback.called) {
            callback.called = true;
            callback(null, result);
        }
    } catch(e) {
        if (!callback.called) {
            callback.called = true;
            callback(e);
        }
    }
};

/**
 * @private
 * @summary Processes messages to be sent back to the supervisor.
 *
 * @param {Buffer} data - The message.
 * @param {function} callback - The callback to call once the message has been sent.
 */
Provider.prototype._processMessage = function(data, callback) {
    this.socket.write(data);
    callback();
};

/**
 * @private
 * @summary Process ping message by responding with a pong.
 *
 * @param {Buffer} data - The ping message.
 * @param {function} callback - The callback to call once the message has been sent.
 */
Provider.prototype._processPing = function(data, callback) {
    console.log('Handling ping...');
    var header = getHeader(action.PONG, 32);
    this.messageQueue.push(Buffer.concat([header, data]));
    callback();
};

module.exports = Provider;