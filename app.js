/*
 * Thinknode App
 */

var Provider = require('./provider');

/**
 * @summary Represents a Thinknode app.
 *
 * @constructor
 */
function App() {}

App.prototype.add = function(a, b) {
    return a + b;
};

var app = new App();
var provider = new Provider({
    app: app
});
provider.start();