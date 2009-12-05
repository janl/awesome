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

exports.flushdb = function() {
  stores[current] = {};
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
  return result;
};

exports.mget = function(keys) {
  var values = [];
  return keys.map(function(key) {
    var value =  exports.get(key);
    if(is_array(value) || is_set(value)) {
      return "";
    }

    return value;
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

exports.randomkey = function() {
  if(store[current] == {}) {
    return "";
  }

  var max = this.dbsize();
  var stop = get_random_int(0, max);
  var counter = 0;
  for(var idx in stores[current]) {
    if(counter == stop) {
      return idx;
    }
    counter = counter + 1;
  }
  return "";
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

exports.save = function() {
  var file = require("file");
  var filename = "dump.jrdb";
  var data = JSON.stringify(store);
  file.write(filename, data).wait();
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

  var real_length = 0;
  for(var idx = 0; idx < value.length; idx++) {
    if(value[idx] !== undefined) {
      real_length++;
    }
  }
  return real_length;
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

exports.lrem = function(key, count, value) {
  if(!this.has(key)) {
    return 0;
  }

  var list = this.get(key);
  if(!is_array(list)) {
    return false;
  }

  count = parseInt(count);
  var stop_at_count = true;
  if(count == 0) {
    stop_at_count = false;
  }

  var deleted = 0;
  if(count >= 0) {
    for(var idx = 0; idx < list.length; idx++) {
      if(list[idx] == value) {
        delete list[idx];
        deleted = deleted + 1;
        count = count - 1;
        if(stop_at_count && count == 0) {
          return deleted;
        }
      }
    }
  } else { // delete from the back
    for(var idx = list.length - 1; idx >= 0; idx--) {
      debug("idx: " + idx);
      if(list[idx] == value) {
        delete list[idx];
        deleted = deleted + 1;
        count = count + 1;
        if(stop_at_count && count == 0) {
          return deleted;
        }
      }
    }
  }
  return deleted;
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

exports.srandmember = function(key) {
  var set = this.get(key);

  if(!is_set(set)) {
    return false;
  }

  var max = this.scard(key) - 1;
  var random = get_random_int(0, max);
  var row = 0;
  for(var idx in set) {
    if(row == random) {
      return idx;
    }
    row = row + 1;
  }
};

exports.sinter = function(keys, dont_convert_to_array) {
  do {
    var first_key = keys.shift();
    var tmp = this.get(first_key);
  } while(!tmp);

  // deep copy sucks
  tmp = JSON.parse(JSON.stringify(tmp));

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

  if(dont_convert_to_array) {
    return tmp;
  }

  var result = [];
  for(var idx in tmp) {
    result.push(idx);
  }

  return result;
};

exports.sunion = function(keys, dont_convert_to_array) {
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

  if(dont_convert_to_array) {
    return union;
  }

  var result = [];
  for(var idx in union) {
    result.push(idx);
  }

  return result;
};

exports.sdiff = function(keys, dont_convert_to_array) {
  var first_key = keys.shift();
  var tmp = this.get(first_key);

  keys.forEach(function(key) {
    var set = this.get(key);
    for(var member in set) {
      if(tmp[member]) {
        delete tmp[member];
      }
    }
  }, this);

  if(dont_convert_to_array) {
    return tmp;
  }

  var result = [];
  for(var idx in tmp) {
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

exports.spop = function(key) {
  if(!this.has(key)) {
    return null;
  }

  var set = this.get(key);
  if(!is_set(set)) {
    return false;
  }

  for(var member in set) {
    delete set[member];
    return member;
  }
};

// sorted sets

exports.zadd = function(key, score, member) {
  var zset = this.get(key);

  if(!zset) {
    zset = ZSet();
  }

  if(!zset.is()) {
    return false;
  }

  if(zset.has(member)) {
    zset.del(member);
  }

  zset.add(score, member);
  return true;
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

exports.sort = function(key, options) {
  var sorter = require("./sorter");
  debug("options in store.js: "+ options)
  sorter.parse(options);

  return sorter.sort(this.get(key), key);
};

// TODO: make private again
exports.is_array = is_array;

// private


function ZSet() {
  this.store = {};

  this.add = function(score, member) {
    this.store[score] = member;
  };

  this.del = function(member) {
    for(var idx in this.store) {
      if(member == store[idx]) {
        delete store[idx];
      }
    }
  };

  this.has = function(member) {
    for(var idx in this.store) {
      if(member == this.store[idx]) {
        return true;
      }
    }
    return false;
  };

  this.is = function() {
    return true;
  };
  return this;
}

// from https://developer.mozilla.org/en/Core_JavaScript_1.5_Reference/Global_Objects/Math/random
function get_random_int(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

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
  sys.puts(s + "\r\n");
}

