/*
  Awesome is a redis clone for node.js

  Initial code by Jan Lehnardt <jan@apache.org>

  MIT License
*/
Object.prototype.toString = function() { return JSON.stringify(this); };

var store = require("./store");
var options = {};

var BY = 0;
var LIMIT = 1;
var GET = 2;
var DIRECTION = 3;
var ALPHA = 4;

exports.parse = function(opts) {
  options = {}; // reset
  debug("options in sorter.sj" + opts);
  // SORT key [BY pattern] [LIMIT start count] [GET pattern] [ASC|DESC] [ALPHA]
  var regex = /(BY [^ ]+ ?)?(LIMIT [^ ]+ [^ ]+ ?)?(GET [^ ]+ ?)?(ASC|DESC)? ?(ALPHA)?/;
  var parts = opts.match(regex).slice(1);
  debug(parts || "null");
  if(!parts) {
    throw("-ERR Awesome SORT: invalid option");
  }

  for(var idx = 0; idx < parts.length; idx++) {
    var part = parts[idx];
    if(!part) { continue; }
    debug("part: " + part);
    switch(idx) {
    case BY:
      options.by = parse_one(part);
      break;
    case LIMIT:
      options.limit = parse_limit(part);
      break;
    case GET:
      options.get = parse_one(part);
      break;
    case DIRECTION:
      options.direction = part;
      break;
    case ALPHA:
      options.ALPHA = part;
      break;
    default:
      throw("-ERR Awesome SORT: invalid option");
    }
  };
};

exports.sort = function(list, key) {
  debug("pre list" + list);

  if(!options.by && !options.get) {
    if(!options.direction || options.direction == "ASC") {
      list.sort();
    } else if(options.direction == "DESC"){
      list.sort();
      list.reverse();
    }
    return list;
  }

  if(options.none) {
    list.sort();
    return list;
  }

  if(options.by) {
    var result = [];
    // fetch KYES with by as the pattern,
    // use the result to sort list
    var sort_by_keys = store.keys(options.by);
    function to_hash(values) {
      var result = {};
      for(var idx = 0; idx < sort_by_keys.length; idx++) {
        var key = sort_by_keys[idx].match(/^[^\*]*(.+)$/)[1];
        result[key] = values[key-1];
      }
      return result;
    }
    var sort_by = to_hash(store.mget(sort_by_keys));

    list.sort(function(a, b) {
      var sort_a = parseInt(sort_by[a]);
      var sort_b = parseInt(sort_by[b]);
      if(sort_a == sort_b) { return 0;}
      return sort_a < sort_b ? -1 : 1;
    });

    if(options.get) { // substitute value-keys with object values
      return list.map(function(elm) {
        if(options.get == "#") {
          return elm;
        }
        var index = options.get.replace("\*", elm);
        return store.lindex(key, index);
      });
    }

    return list;
  }
}

// private

function parse_one(one) {
  return one.split(" ")[1];
}

function parse_limit(limit) {
  var split = limit.split(" ");
  return {from: split[1], to: split[2]};
}
var puts = require("sys").puts;

function test() {
  exports.parse("DESC"); puts(options);
  exports.parse("LIMIT 0 10"); puts(options);
  exports.parse("LIMIT 0 10 DESC ALPHA"); puts(options);
  exports.parse("BY weight_*"); puts(options);
  exports.parse("BY weight_* GET object_*"); puts(options);
  exports.parse("BY weight_* GET object_* GET #"); puts(options);
}

// test();

function debug(s) {
  puts(s + "\r\n");
}