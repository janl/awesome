/*
  Awesome is a redis clone for node.js

  Initial code by Jan Lehnardt <jan@apache.org>

  MIT License
*/

var PORT = 6379;

var tcp = require("tcp");
var sys = require("sys");

var enable_debug = true;

var store = require("./store");

var server = tcp.createServer(function(socket) {
  // requests and responses have this as a trailer
  var eol = "\r\n";

  var EMPTY_VALUE = {};
  var E_VALUE = "Operation against a key holding the wrong kind of value";

  var reply = {
    send: function(s) {
      // debug("reply: '" + s + "'");
      socket.send(s + eol);
    },

    ok: function() {
      reply.send("+OK")
    },

    bulk: function(s) {
      reply.send("$" + s.toString().length);
      reply.send(s);
    },

    empty_bulk: function() {
      reply.send("$-1");
    },

    multi_bulk: function(values) {
      reply.send("*" + values.length);
      values.forEach(function(value) {
        reply.bulk(value);
      });
    },

    number: function(n) {
      reply.send(":" + n);
    },

    error: function(s) {
      reply.send("-ERR " + s);
    },

    bool: function(bool) {
      var code = bool ? 1 : 0;
      reply.send(":" + code);
    },

    nil: function(s) {
      reply.send("$-1");
    },

    list: function(value, reply_function) {
      if(value === false) {
        this.error(E_VALUE);
      } else if(value === null) {
        this.nil();
      } else {
        reply_function(value);
      }
    },

    status: function(s) {
      reply.send("+" + s);
    }
  };

  function Command(line) {

    function parseCommand(s) {
      var cmd = "";
      for(var idx = 0; idx < s.length; ++idx) {
        var chr = s[idx];
        if(chr == " " || chr == "\r" || chr == "\n") {
          return cmd;
        }
        cmd += chr;
      }
      return cmd;
    }

    function parseArgs(s) {
      var args = [];
      var arg = "";
      var argidx = 0;
      for(var idx = 0; idx < s.length; ++idx) {
        var chr = s[idx];
        if(chr == " " || chr == "\r" || chr == "\n") {
          if(arg) {
            args.push(arg);
            argidx = argidx + 1;
            arg = "";
          }
        } else {
          arg += chr;
        }
      }
      return args;
    }

    this.cmd = parseCommand(line).toLowerCase();
    this.args = parseArgs(line);

    var that = this;

    var callbacks = {
      // keep sorted alphabetically
      // list-related functions at the end
      dbsize: {
        callback: function() {
          debug("received DBSIZE command");
          var size = store.dbsize();
          reply.send(":" + size);
        }
      },

      del: {
        callback:function() {
          debug("received DEL command");
          if(that.args.length > 2) {
            var keys = that.args.slice(1);
            var deleted = store.del(keys);
            reply.send(":" + deleted);
          } else {
            var key = that.args[1];
            if(store.has(key)) {
              store.del(key);
              reply.bool(true);
            } else {
              reply.bool(false);
            }
          }
        }
      },

      flushdb: {
        callback: function() {
          debug("received FLUSHDB command");
          store.flushdb();
          reply.ok();
        }
      },

      get: {
        callback: function() {
          debug("received GET command");
          var key = that.args[1];
          if(store.has(key)) {
            var value = store.get(key);
            if(EMPTY_VALUE === value) {
              // empty value
              reply.bulk("");
            } else {
              reply.bulk(value);
            }
          } else { // not found
            reply.nil();
          }
        }
      },

      getset: {
        bulk: true,
        callback: function() {
          debug("received GETSET command");
          var key = that.args[1];
          if(store.has(key)) {
            var value = store.get(key);
            reply.send("$" + value.length);
            reply.send(value);
          } else { // not found
            reply.nil();
          }
        }
      },

      incr: {
        callback: function() {
          var key = that.args[1];
          var value = store.incr(key);
          reply.send(":" + value);
        }
      },

      decr: {
        callback: function() {
          var key = that.args[1];
          var value = store.decr(key);
          reply.send(":" + value);
        }
      },

      incrby: {
        callback: function() {
          var key = that.args[1];
          var increment = that.args[2];
          var value = store.incrby(key, increment);
          reply.send(":" + value);
        }
      },

      decrby: {
        callback: function() {
          var key = that.args[1];
          var decrement = that.args[2];
          var value = store.decrby(key, decrement);
          reply.send(":" + value);
        }
      },

      exists: {
        callback: function() {
          debug("received EXISTS command");
          var key = that.args[1];
          if(store.has(key)) {
            reply.bool(true);
          } else {
            reply.bool(false);
          }
        }
      },

      info: {
        callback: function() {
          debug("received INFO command");
          // TODO
          /*
          edis_version:0.07
          connected_clients:1
          connected_slaves:0
          used_memory:3187
          changes_since_last_save:0
          last_save_time:1237655729
          total_connections_received:1
          total_commands_processed:1
          uptime_in_seconds:25
          uptime_in_days:0          
          */
          var info = [
            "redis_version:0.07",
            "connected_clients:1",
            "connected_slaves:0",
            "used_memory:3187",
            "changes_since_last_save:0",
            "last_save_time:1237655729",
            "total_connections_received:1",
            "total_commands_processed:1",
            "uptime_in_seconds:25",
            "uptime_in_days:0",
            "bgsave_in_progress:0",
          ];
          reply.bulk(info);
        }
      },

      keys: {
        callback: function() {
          debug("received KEYS command");
          var pattern = that.args[1] || '*';
          var result = store.keys(pattern).join(" ");
          reply.bulk(result);
        }
      },

      mget: {
        callback: function() {
          debug("received MGET command");
          var keys = that.args.slice(1);
          var values = store.mget(keys);
          reply.send("*" + values.length);
          values.forEach(function(value) {
            if(value) {
              reply.bulk(value);
            } else {
              reply.nil();
            }
          });
        }
      },

      move: {
        callback: function() {
          debug("received MOVE command");
          var key = that.args[1];
          var dbindex = that.args[2];
          reply.bool(store.move(key, dbindex));
        }
      },

      mset: {
        bulk: true,
        callback: function() {
          debug("received MSET command");
          var msets = that.multi_data;
          for(var idx in msets) {
            store.set(idx, msets[idx]);
          }
          reply.ok();
        }
      },

      ping: {
        callback: function() {
          debug("received PING command");
          reply.send("+PONG");
        }
      },

      quit: {
        callback: function() {
          debug("received QUIT command");
          socket.close();
        }
      },

      rename: {
        callback: function() {
          debug("received RENAME command");
          var src = that.args[1];
          var dst = that.args[2];
          if(src == dst) {
            reply.error("source and destination objects are the same");
          } else if(!store.has(src)) {
            reply.error("no such key");
          } else {
            store.rename(src, dst);
            reply.ok();
          }
        }
      },

      renamenx: {
        callback: function() {
          debug("received RENAMENX command");
          var src = that.args[1];
          var dst = that.args[2];
          if(src == dst) {
            reply.error("source and destination objects are the same");
          } else {
            if(store.rename(src, dst, true)) {
              reply.bool(true);
            } else {
              reply.bool(false);
            }
          }
        }
      },

      select: {
        callback: function() {
          debug("received SELECT command");
          var index = that.args[1];
          store.select(index);
          reply.ok();
        }
      },

      set: {
        bulk: true,
        callback: function() {
          debug("received SET command");
          var key = that.args[1];
          var value = that.data || EMPTY_VALUE;
          store.set(key, value);
          reply.ok();
        }
      },

      setnx: {
        bulk: true,
        callback: function() {
          debug("received SETNX command");
          var key = that.args[1];
          if(!store.has(key)) {
            store.set(key, that.data);
            reply.bool(true);
          } else {
            reply.bool(false);
          }
        }
      },

      sort: {
        callback: function() {
          var key = that.args[1];
          var options = that.args.slice(2).join(" ");
          debug("options in awseome.js: " + options);
          var result = store.sort(key, options);
          debug("result:");
          debug(result);
          debug(typeof result);
          reply.multi_bulk(result);
        }
      },

      type: {
        callback: function() {
          var key = that.args[1];
          reply.status(store.type(key));
        }
      },

      // list related functions
      lindex: {
        callback: function() {
          debug("received LINDEX command");
          var key = that.args[1];
          var index = parseInt(that.args[2]);

          if(isNaN(index)) {
            // Redis assumes index 0 when anything but an 
            // integer is passed as the index
            index = 0;
          }

          if(store.has(key)) {
            var value = store.lindex(key, index);
            reply.list(value, reply.bulk);
          }
        }
      },

      llen: {
        callback: function() {
          debug("received LLEN command");
          var key = that.args[1];
          var value = store.llen(key);
          if(value === false) {
            reply.error(E_VALUE);
          } else {
            reply.send(":" + value);
          }
        }
      },

      lpush: {
        bulk: true,
        callback: function() {
          debug("received LPUSH command");
          var key = that.args[1];
          var value = that.data || EMPTY_VALUE;
          if(store.lpush(key, value)) {
            reply.ok();
          } else {
            reply.error(E_VALUE);
          }
        }
      },

      lpop: {
        callback: function() {
          debug("received LPOP command");
          var key = that.args[1];
          var value = store.lpop(key);
          reply.list(value, reply.bulk);
        }
      },

      rpush: {
        bulk: true,
        callback: function() {
          debug("received RPUSH command");
            var key = that.args[1];
            var value = that.data || EMPTY_VALUE;
            if(store.rpush(key, value)) {
              reply.ok();
            } else {
              reply.error(E_VALUE);
            }
          }
      },

      rpop: {
        callback: function() {
          debug("received RPOP command");
            var key = that.args[1];
            var value = store.rpop(key);
            reply.list(value, reply.bulk);
          }
      },

      rpoplpush: {
        bulk: true,
        callback: function() {
          debug("received RPOPLPUSH command");
          var src = that.args[1];
          var dst = that.data;

          var value = store.rpop(src);
          if(value === null) {
            reply.nil();
          } else if(value === false) {
            reply.error(E_VALUE);
          } else {
            if(store.lpush(dst, value)) {
              reply.bulk(value);
            } else {
              // restore src
              store.rpush(src, value);
              reply.error(E_VALUE);
            }
          }
        }
      },

      lrange: {
        callback: function() {
          debug("received LRANGE command");
          var key = that.args[1];
          var start = that.args[2];
          var end = that.args[3];
          var value = store.lrange(key, start, end);
          if(value === null) {
            reply.empty_bulk();
          } else {
            reply.multi_bulk(value);
          }
        }
      },

      lset: {
        bulk: true,
        callback: function() {
          debug("received LSET command");
          var key = that.args[1];
          var index = that.args[2];
          var value = that.data;

          var result = store.lset(key, index, value);
          if(result === null) {
            reply.error("no such key");
          } else if(result === undefined) {
            reply.error("index out of range");
          } else if(result === false) {
            reply.error(E_VALUE);
          } else {
            reply.ok();
          }
        }
      },

      ltrim: {
        callback: function() {
          debug("received LTRIM command");
          var key = that.args[1];
          var start = that.args[2];
          var end = that.args[3];
          var status = store.ltrim(key, start, end);
          if(status === null) {
            reply.error("no such key");
          } else if(status === false) {
            reply.error(E_VALUE);
          } else {
            reply.ok();
          }
        }
      },

      sadd: {
        bulk: true,
        callback: function() {
          debug("received SADD command");
          var key = that.args[1];
          var member = that.data;
          var result = store.sadd(key, member);
          if(result === null) { // the key is already in the set
            reply.bool(false);
          } else if (result === false) {
            reply.error(E_VALUE);
          } else {
            reply.bool(true);
          }
        }
      },

      scard: {
        callback: function() {
          debug("received SCARD command");
          var key = that.args[1];
          var result = store.scard(key);
          reply.send(":" + result);
        }
      },

      sismember: {
        bulk: true,
        callback: function() {
          debug("received SISMEMBER command");
          var key = that.args[1];
          var member = that.data;
          reply.bool(store.sismember(key, member));
        }
      },

      smembers: {
        callback: function() {
          debug("received SMEMBERS command");
          var key = that.args[1];
          var members = store.smembers(key);
          reply.multi_bulk(members);
        }
      },

      srem: {
        bulk: true,
        callback: function() {
          debug("received SREM command");
          var key = that.args[1];
          var member = that.data;
          var result = store.srem(key, member);
          if(result === null) {
            reply.error(E_VALUE);
          } else {
            reply.bool(result);
          }
        }
      },

      sdiff: {
        callback: function() {
          var keys = that.args.slice(1);
          var result = store.sdiff(keys);
          reply.multi_bulk(result);
        }
      },

      sinter: {
        callback: function() {
          var keys = that.args.slice(1);
          var result = store.sinter(keys);
          reply.multi_bulk(result);
        }
      },

      sinterstore: {
        callback: function() {
          var dst = that.args[1];
          var keys = that.args.slice(2);
          var result = store.sinter(keys, true);
          if(result) {
            store.set(dst, result);
            reply.number(result.length);
          } else {
            reply.ok();
          }
        }
      },

      sunion: {
        callback: function() {
          var keys = that.args.slice(1);
          var result = store.sunion(keys);
          reply.multi_bulk(result);
        }
      },

      sunionstore: {
        callback: function() {
          var dst = that.args[1];
          var keys = that.args.slice(2);
          var result = store.sunion(keys, true);
          if(result) {
            store.set(dst, result);
            reply.number(result.length);
          } else {
            reply.ok();
          }
        }
      },

      sdiffstore: {
        callback: function() {
          var dst = that.args[1];
          var keys = that.args.slice(2);
          var result = store.sdiff(keys, true);
          if(result) {
            store.set(dst, result);
            reply.number(result.length);
          } else {
            reply.ok();
          }
        }
      },

      spop: {
        callback: function() {
          debug("received SPOP command");
          var key = that.args[1];
          var result = store.spop(key);
          if(result === null) {
            reply.nil();
          } else if(result === false) {
            reply.error(E_VALUE);
          } else {
            reply.bulk(result);
          }
        }
      },

      srandmember: {
        callback: function() {
          var key = that.args[1];
          var value = store.srandmember(key);
          if(key === false) {
            reply.error(E_VALUE);
          } else {
            reply.bulk(value);
          }
        }
      },

      // sorted sets
      zadd: {
        bulk: true,
        callback: function() {
          var key = that.args[1];
          var score = that.args[2];
          var member = that.args[3];
          var result = store.zadd(key, score, member);
          if(result === false) {
            reply.error(E_VALUE);
          } else {
            reply.bool(!!result);
          }
        }
      },

      // storage
      save: {
        callback: function() {
          store.save();
          reply.ok();
        }
      },

      // for debugging
      dump: {
        callback: function() {
          debug("received DUMP command");
          sys.print(store.dump() + eol);
          reply.ok();
        }
      },

    };

    this.is_inline = function() {
      if(typeof callbacks[this.cmd] === "undefined") {
        return true; // unkown cmds are inline
      }

      return !callbacks[this.cmd].bulk;
    };

    this.setData = function(data) {
      this.data = data.trim();
    },

    this.setMultiBulkData = function(lines) {
      lines = lines.slice(1);
      var result = {};
      var key = null;
      for(var idx = 0; idx < lines.length; idx++) {
        if(idx % 2) { // skip even lines
          if(!key) {
            key = lines[idx];
          } else {
            result[key] = lines[idx];
            key = null;
          }
        }
      }
      this.multi_data = result;
    },

    this.exec = function() {
      debug("in exec '" + this.cmd + "'");
      if(callbacks[this.cmd]) {
        callbacks[this.cmd].callback(this.args);
      } else {
        reply.error("unknown command");
      }
    };

    return this;
  }

  socket.setEncoding("utf8"); // check with redis protocol

  function debug(s) {
    if(enable_debug && s !== null) {
      sys.print(s.toString().substr(0,128) + eol);
    }
  }

  // for reading requests

  function adjustBuffer(buffer) {
    return buffer.substr(buffer.indexOf(eol) + 2)
  }

  function parseData(s) {
    var start = s.indexOf(eol) + 2;
    return s.substring(start, s.indexOf(eol, start));
  }

  function string_count(haystack, needle) {
    var regex = new RegExp(needle, "g");
    var result = haystack.match(regex);
    if(result) {
      return result.length - 1;
    } else {
      return 0;
    }
  }

  var buffer = "";
  var in_bulk_request = false;
  var in_multi_bulk_request = false;
  var cmd = {};
  socket.addListener("receive", function(packet) {
    buffer += packet;
    debug("read: '" + buffer.substr(0, 64) + "'");
    while(buffer.indexOf(eol) != -1) { // we have a newline
      if(in_multi_bulk_request) {
        debug("in multi bulk request");
        // handle multi bulk requests
        if(!cmd_length) {
          var cmd_length = cmd.len * 2; // *2 = $len
        }
        debug("cmd_length: " + cmd_length);
        debug("string_count: " + string_count(buffer, eol));
        if(string_count(buffer, eol) == cmd_length) {
          var lines = buffer.split(eol);
          cmd.cmd = lines[2];
          lines = lines.slice(2); // chop off *len\n$4\nmget
          cmd.setMultiBulkData(lines)
          cmd_length++;
          while(cmd_length--) {
            buffer = adjustBuffer(buffer);
          }
          in_multi_bulk_request = false;
          cmd.exec();
          
        }
      } else {
        // handle bulk requests
        if(in_bulk_request) {
          debug("in bulk req");
          cmd.setData(buffer);
          in_bulk_request = false;
          buffer = adjustBuffer(buffer);
          cmd.exec();
        } else {
          // not a bulk request yet
          debug("not in bulk req (yet)");
          cmd = Command(buffer);
          if(cmd.cmd.charAt(0) == "*") {
            cmd.len = cmd.cmd.charAt(1);
            in_multi_bulk_request = true;
            continue;
          }
          if(cmd.is_inline()) {
            debug("is inline command");
            cmd.exec();
          } else {
            if(buffer.indexOf(eol) != buffer.lastIndexOf(eol)) { // two new lines
              debug("received a bulk command in a single buffer");
              // parse out command line
              cmd.setData(parseData(buffer));
              in_bulk_request = false;
              buffer = adjustBuffer(buffer);
              cmd.exec();
            } else {
              debug("wait for bulk: '" + buffer + "'");
              in_bulk_request = true;
            }
          }
          buffer = adjustBuffer(buffer);
        }
      }
    }
  });

  socket.addListener("eof", function() {
    socket.close();
  });
});

server.listen(PORT, "localhost");
