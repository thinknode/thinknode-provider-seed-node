/*
 * Thinknode App
 */

var bluebird = require('bluebird');
var sleep = require('system-sleep');

var Provider = require('./provider');

var app = {};

app.add = function(a, b) {
    return a + b;
};

app.add_async = function(a, b) {
    return bluebird.resolve(a + b);
};

app.add_with_progress = function(a, b, progress) {
    progress(0.25, "25%");
    sleep(1000);
    progress(0.5, "50%");
    sleep(1000);
    progress(0.75);
    sleep(1000);
    return a + b;
};

app.add_with_failure = function(a, b, progress, fail) {
    fail("my_error", "This is a test of the error functionality");
};

app.get_blob_length = function(a) {
    return a.length;
};

app.get_hours = function(a) {
    return a.getUTCHours();
};

var provider = new Provider(app);
provider.start();
