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
      debug("reply: '" + s || "null" + "'");
      socket.write(s + eol);
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
      var real_length = 0;
      for(var idx = 0; idx < values.length; idx++) {
        if(values[idx] !== undefined) {
          real_length++;
        }
      }
      reply.send("*" + real_length);
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
      debug("in parse command: " + s);
      var cmd = "";
      for(var idx = 0; idx < s.length; idx++) {
        var chr = s[idx];
        if(chr == " " || chr == "\r" || chr == "\n") {
          debug("parsed command: " + cmd);
          return cmd;
        }
        cmd += chr;
      }

      debug("parsed command: " + cmd);
      return cmd;
    }

    function parseArgs(s) {
      var args = [];
      var arg = "";
      var argidx = 0;
      for(var idx = 0; idx < s.length; idx++) {
        var chr = s[idx];
        debug("doing chr: " + chr);
        if(chr == " " || chr == "\r" || chr == "\n") {
          if(arg) {
            args.push(arg);
            argidx = argidx + 1;
            arg = "";
            if(chr == "\r" && type() == "inline") {
              return args;
            }
          }
        } else {
          arg += chr;
        }
      }
      debug("cmdline: " + s);
      debug("parsed args: " + args);
      return args;
    }

    this.data = "";
    this.setData = function(data) {
      var idx = 0;
      while(this.data.length < data_length) {
        debug("adding char: " + data.charAt(idx));
        this.data += data.charAt(idx);
        idx = idx + 1;
      }
    };

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
    };

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
            reply.number(deleted);
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
          var value = that.data;
          var old_value = store.get(key);
          store.set(key, value);
          if(old_value === null) {
            reply.nil();
          } else {
            reply.send("$" + old_value.length);
            reply.send(old_value);
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
          debug("msets: " + msets);
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
            debug("value: " + value);
            reply.multi_bulk(value);
          }
          debug("replied");
        }
      },

      lrem: {
        bulk: true,
        callback: function() {
          debug("received LREM comand");
          var key = that.args[1];
          var count = parseInt(that.args[2]);
          var value = that.data;
          var result = store.lrem(key, count, value);
          if(result === false) {
            reply.error(E_VALUE);
          } else {
            reply.number(result);
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

      randomkey: {
        callback: function() {
          debug("received RANDOMKEY command");
          var value = store.randomkey();
          reply.status(value);
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

      smove: {
        bulk: true,
        callback: function() {
          debug("received SMOVE command");
          var src = that.args[1];
          var dst = that.args[2];
          var member = that.data;
          var result = store.smove(src, dst, member);
          if(result === false) {
            reply.error(E_VALUE);
          } else {
            reply.bool(result !== null);
          }
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

    this.cmd = parseCommand(line).toLowerCase();
    this.args = parseArgs(line);
    debug("args: " + this.args);

    this.data_length = 0;
    if(type() == "bulk") {
      this.data_length = parseInt(this.args.pop());
      debug("data_length" + this.data_length);
    }

    this.cmd_length = 0;
    if(type() == "multi") {
      this.cmd_length = parseInt(this.cmd.substr(1));
    }

    this.inner_cmd = "";
    this.inner_cmd_length = 0;


    function type() {
      debug("type: cmd: " + this.cmd);
      if(this.cmd.charAt(0) == "*") {
        return "multi";
      } else if(is_inline()) {
          return "inline";
      } else {
        return "bulk";
      }
    }; this.type = type;

    function is_inline() {
      if(typeof callbacks[this.cmd] === "undefined") {
        return true; // unkown cmds are inline
      }

      return !callbacks[this.cmd].bulk;
    }; this.is_inline = is_inline;

    this.ready = function() {
      switch(that.type()) {
      case "inline":
        return true;
      case "bulk":
        // if that.data is set and not empty, we're ready
        debug("bulk ready: '" + that.data + "'");
        if(this.data_length == 0) {
          // special case for empty data strings
          return true;
        }

        return that.data && (that.data.length == this.data_length); 
      case "multi":
        // debug("is multi");
        // if we have a command, and have args equal to cmd_length
        return that.inner_cmd && (that.args.length == that.cmd_length);
      default:
        return false;
      }
    };

    this.parse_inner_cmd_length = function(buffer) {
      var len = buffer.match(/\$(\d+)\r\n/)[1];
      if(!that.inner_cmd_length) {
        this.inner_cmd_length = parseInt(len);
      }
    };

    this.parse_inner_cmd = function(buffer) {
      this.inner_cmd = buffer.substr(0, this.inner_cmd_length);
    };

    var current_arg_length = 0;
    this.parse_multi_args = function(buffer) {
      if(!current_arg_length) {
        // if we don't have an arg length, try reading that
        if(buffer.indexOf(eol) === -1) {
          // if there is no newline, keep buffering
          return 0;
        } else {
          // read the arg length
          current_arg_length = read_length(buffer);
          var bytes = current_arg_length.length;
          current_arg_length = parseInt(current_arg_length);
          // and return bytes read
          return bytes;
        }
      } else {
        if(buffer.length < current_arg_length) {
          // keep buffering
          return 0;
        } else {
          // set arg
          this.args.push(buffer.trim());
          // reset arg length
          current_arg_length = 0;
          // return bytes read
          return buffer.length;
        }
      }
    };

    function read_length(line) {
      return buffer.match(/\$(\d+)\r\n/)[1];
    }

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

  function adjustBuffer(buffer, idx) {
    debug("adjust buffer idx:" + idx);
    return buffer.substr(idx + 1);
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
  var cmd = null;
  socket.addListener("data", function(packet) {
    buffer += packet;
    debug("read: '" + buffer.substr(0, 128) + "'");

    var eol_pos;
    while(buffer.length > 0) {
      eol_pos = buffer.indexOf(eol);
      if(!cmd) {
        // parse line until eol, extract cmd name, args and data length if it's a bulk
        var line = buffer.substr(0, eol_pos + eol.length);
        debug("line: '" + line + "'");
        debug("eol_pos: " + eol_pos);
        cmd = Command(line);
        debug("buffer before adjust:'" + buffer + "'");
        buffer = adjustBuffer(buffer, eol_pos + 1);
        debug("buffer after adjust:'" + buffer + "'");
      }

      if(cmd.type() == "bulk") {
        // raed chars until cmd.data_length
        if(buffer.length < cmd.data_length) {
          debug("!!moar buffer");
          // not enough data to read in the buffer
          return;
        }
        cmd.setData(buffer);
        debug("buffer before adjust:'" + buffer + "'");
        buffer = adjustBuffer(buffer, cmd.data_length + 1);
        debug("buffer after adjust:'" + buffer + "'");
      }

      if(cmd.type() == "multi") {
        debug("is multi");
        if(!cmd.inner_cmd_length) {
          debug("parse inner command length");
          // parse inner command length
          cmd.parse_inner_cmd_length(buffer);
          debug("inner cmd length: " + cmd.inner_cmd_length);
          buffer = adjustBuffer(buffer, cmd.inner_cmd_length);
        } else if(!cmd.inner_cmd) {
          debug("parse inner command");
          // parse inner command
          cmd.parse_inner_cmd(buffer);
          debug("inner cmd: " + cmd.inner_cmd);
          buffer = adjustBuffer(buffer, cmd.inner_cmd_length);
        } else {
          // parse args
          debug("parse args");
          var parsed = cmd.parse_multi_args(buffer);
          debug("parsed: " + parsed);
          buffer = adjustBuffer(buffer, parsed);
        }
      }

      if(cmd && cmd.ready()) {
        debug("ready: " + cmd.cmd);
        // fire!
        cmd.exec();
        cmd = null; // reset
      }
    }
  });

  socket.addListener("eof", function() {
    socket.close();
  });
});

server.listen(PORT, "localhost");
