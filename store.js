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
  delete stores[current][key];
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
}
