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

exports.has = function(key, dbindex) {
  dbindex = dbindex || current;
  return !!stores[dbindex][key];
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

exports.move = function(key, dbindex) {
  if(!this.has(key)) {
    return false;
  }

  if(this.has(key, dbindex)) {
    return false;
  }

  var value = this.get(key);
  this.set(key, value, dbindex);
  this.del(key);
};

exports.rename = function(src, dst, do_not_overwrite) {
  if(do_not_overwrite) {
    if(this.has(dst)) {
      return false;
    }
  }
  this.set(dst, this.get(src));
  this.del(src);
};

exports.select = function(index) {
  current = index;
  if(!stores[current]) {
    stores[current] = {};
  }
};

exports.set = function(key, value, dbindex) {
  dbindex = dbindex || current;
  stores[dbindex][key] = value;
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
    return null;
  }

  return value.shift();
};

exports.rpush = function(key, value) {
  if(!this.has(key)) {
    this.set(key, []);
  }

  var list = this.get(key);
  if(!this.is_array(list)) {
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
    return null;
  }

  return value.pop();
};

exports.lrange = function(key, start, end) {
  if(!this.has(key)) {
    return null;
  }
  var value = this.get(key);
  if(!value || !this.is_array(value)) {
    return null;
  }

  var start = parseInt(start);
  var end = parseInt(end);

  if(end < 0) { // With Array.slice(start, end), end is non-inclusive
    end = value.length + end + 1; // we need inclusive
  }

  if(start == end) {
    end = end + 1; // eeeenclusive
  }
  var slice =  value.slice(start, end);
  return slice;
};

exports.lset = function(key, index, value) {
  if(!this.has(key)) {
    return null;
  }

  var list = this.get(key);
  if(!is_array(list)) {
    return false;
  }
  index = parseInt(index);
  if(index < 0) {
    index = list.length + index; // support negative indexes
  }

  if(index < list.length) {
    list[index] = value;
    return true;
  }
};

exports.ltrim = function(key, start, end) {
  var value = this.lrange(key, start, parseInt(end) + 1);
  if(value) {
    this.set(key, value);
  }
  return value;
};

exports.sadd = function(key, member) {
  var set = this.get(key);

  if(!set) {
    set = {};
  }

  if(!is_set(set)) {
    return false;
  }

  if(set[member]) {
    return null;
  }

  set[member] = true;
  this.set(key, set);
  return true;
};

exports.scard = function(key) {
  if(!this.has(key)) {
    return 0;
  }

  var card = 0;
  for(var idx in this.get(key)) {
    card++;
  }
  return card;
};

exports.sismember = function(key, member) {
  if(this.has(key)) {
    var value = this.get(key);
    return !!value[member];
  }

  return false;
};

exports.smembers = function(key) {
  var value = this.get(key);
  if(!value) {
    value = {};
  }
  var result = [];
  for(var idx in value) {
    result.push(idx);
  }
  return result;
};

exports.sinter = function(keys) {
  do {
    var first_key = keys.shift();
    var tmp = this.get(first_key);
  } while(!tmp);

  keys.forEach(function(key) {
    var set = this.get(key);
    if(!set) {
      // empty intersection
      return [];
    }

    for(var member in tmp) {
      if(!set[member]) {
        delete tmp[member];
      }
    }
  }, this);

  var result = [];
  for(var idx in tmp) {
    result.push(idx);
  }

  return result;
};

exports.sunion = function(keys) {
  var union = {};
  keys.forEach(function(key) {
    var set = this.get(key);
    if(!set) {
      return;
    }
    for(var member in set) {
      union[member] = true;
    }
  }, this);

  var result = [];
  for(var idx in union) {
    result.push(idx);
  }

  return result;
};

exports.srem = function(key, member) {
  var set = this.get(key);

  if(!is_set(set)) {
    return false;
  }

  if(set[member]) {
    delete set[member];
    return true;
  }

  return false;
};

exports.type = function(key) {
  if(!this.has(key)) {
    return "none";
  }

  var value = this.get(key);
  if(is_array(value)) {
    return "list";
  }

  if(is_set(value)) {
    return "set";
  }

  return "string";
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

function is_set(s) {
  if(is_array(s)) {
    return false;
  }

  return (s !== null && typeof s === 'object')
}

function debug(s) {
  sys.print(s + "\r\n");
}

