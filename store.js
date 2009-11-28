/*
  Awesome is a redis clone for node.js

  Initial code by Jan Lehnardt <jan@apache.org>

  MIT License
*/
var sys = require("sys");

var stores = [];
var current = 0;
stores[current] = {};

exports.dbsize = function() {
  var size = 0;
  var store = stores[current];
  for(var key in store) {
    if(store.hasOwnProperty(key)) {
      size = size + 1;
    }
  }
  return size;
};

exports.del = function(key) {
  var deleted = 0;
  if(is_array(key)) {
    key.forEach(function(k) {
      if(exports.has(k)) {
        deleted++;
      }
      delete stores[current][k];
    });
  } else {
    delete stores[current][key];
  }
  return deleted;
};

exports.dump = function() {
  return JSON.stringify(stores);
};

exports.get = function(key) {
  return stores[current][key] || null;
};

exports.has = function(key) {
  return !!stores[current][key];
};

exports.incr = function(key) {
  if(typeof stores[current][key] == "undefined") {
    exports.set(key, 0);
  }
  stores[current][key] = parseInt(stores[current][key]) + 1;
  return stores[current][key];
};

exports.decr = function(key) {
  if(typeof stores[current][key] == "undefined") {
    exports.set(key, 0);
  }
  stores[current][key] = parseInt(stores[current][key]) - 1;

  return stores[current][key];
};

exports.incrby = function(key, increment) {
  if(typeof stores[current][key] == "undefined") {
    exports.set(key, 0);
  }
  stores[current][key] = parseInt(stores[current][key]) + parseInt(increment);
  return stores[current][key];
};

exports.decrby = function(key, decrement) {
  if(typeof stores[current][key] == "undefined") {
    exports.set(key, 0);
  }
  stores[current][key] = parseInt(stores[current][key]) - parseInt(decrement);

  return stores[current][key];
};

exports.keys = function(pattern) {
  var store = stores[current];
  var result = [];
  for(var key in store) {
    if(store.hasOwnProperty(key)) {
      if(keymatch(key, pattern)) {
        result.push(key);
      }
    }
  }
  return result.join(" ");
};

exports.mget = function(keys) {
  var values = [];
  return keys.map(function(key) {
    return exports.get(key);
  });
};

exports.select = function(index) {
  current = index;
  if(!stores[current]) {
    stores[current] = {};
  }
};

exports.set = function(key, value) {
  stores[current][key] = value;
};

// list functions

exports.lindex = function(key, index) {
  if(!this.has(key)) {
    return null;
  }

  var value = this.get(key);
  if(!this.is_array(value)) {
    return false;
  }

  if(index < 0) { // support negative int wrapping
    index = value.length + index;
  }

  if(index < 0 || index > value.length) { // out of bound returns the empty string
    return "";
  }

  return value[index];
};

exports.llen = function(key) {
  if(!this.has(key)) {
    return 0;
  }

  var value = this.get(key);
  if(!is_array(value)) {
    return false;
  }

  return value.length;
}

exports.lpush = function(key, value) {
  if(!this.has(key)) {
    this.set(key, []);
  }

  var list = this.get(key);
  if(!this.is_array(list)) {
    debug("not a list");
    return false;
  }

  list.unshift(value);
  this.set(key, list);
  return true;
};

exports.lpop = function(key) {
  if(!this.has(key)) {
    return null;
  }

  var value = this.get(key);
  if(!this.is_array(value)) {
    return false;
  }

  if(value.length == 0) {
    return false;
  }

  return value.shift();
};

exports.rpush = function(key, value) {
  if(!this.has(key)) {
    this.set(key, []);
  }

  var list = this.get(key);
  if(!this.is_array(list)) {
    debug("not a list");
    return false;
  }

  list.push(value);
  this.set(key, list);
  return true;
};

exports.rpop = function(key) {
  if(!this.has(key)) {
    return null;
  }

  var value = this.get(key);
  if(!this.is_array(value)) {
    return false;
  }

  if(value.length == 0) {
    return false;
  }

  return value.pop();
};

// TODO: make private again
exports.is_array = is_array;

// private

function keymatch(key, pattern) {
  // shortcut
  if(pattern == "*") {
    return true;
  }

  if(pattern.substr(-1) == "*") {
    var prefix = pattern.substr(0, pattern.length - 1);
    return key.substr(0, prefix.length) == prefix;
  }
}

/*
  Thanks Doug Crockford
  JavaScript — The Good Parts lists an alternative that works better with
  frames. Frames can suck it, we use the simple version.
*/
function is_array(a) {
  return (a &&
    typeof a === 'object' &&
    a.constructor === Array);
}

function debug(s) {
  sys.print(s + "\r\n");
}

