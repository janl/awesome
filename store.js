/*
  Awesome is a redis clone for node.js

  Initial code by Jan Lehnardt <jan@apache.org>

  MIT License
*/

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
}

exports.dump = function() {
  return JSON.stringify(stores);
}

exports.get = function(key) {
  return stores[current][key] || false;
};

exports.has = function(key) {
  return !!stores[current][key];
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
}

exports.select = function(index) {
  current = index;
  if(!stores[current]) {
    stores[current] = {};
  }
}

exports.set = function(key, value) {
  stores[current][key] = value;
};

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

