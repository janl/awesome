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
  var ok = "+OK" + eol;

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

    function reply(line) {
      socket.send(line + eol);
    }

    this.cmd = parseCommand(line).toLowerCase();
    this.args = parseArgs(line);

    var that = this;

    var callbacks = {
      // keep sorted alphabetically
      dbsize: {
        inline: true,
        callback: function() {
          debug("received DBSIZE command");
          var size = store.dbsize();
          reply(":" + size);
        }
      },

      del: {
        inline: true,
        callback:function() {
          debug("received DEL command");
          if(that.args.length > 2) {
            var keys = that.args.slice(1);
            var deleted = store.del(keys);
            reply(":" + deleted);
          } else {
            var key = that.args[1];
            if(store.has(key)) {
              store.del(key);
              reply(":1");
            } else {
              reply(":0");
            }
          }
        }
      },

      get: {
        inline: true,
        callback: function() {
          debug("received GET command");
          var key = that.args[1];
          if(store.has(key)) {
            var value = store.get(key);
            reply("$" + value.toString().length);
            reply(value);
          } else { // not found
            reply("$-1");
          }
        }
      },

      getset: {
        inline: false,
        callback: function() {
          debug("received GETSET command");
          var key = that.args[1];
          if(store.has(key)) {
            var value = store.get(key);
            reply("$" + value.length);
            reply(value);
          } else { // not found
            reply("-1");
          }
        }
      },

      incr: {
        inline: true,
        callback: function() {
          var key = that.args[1];
          var value = store.incr(key);
          reply(":" + value);
        }
      },

      decr: {
        inline: true,
        callback: function() {
          var key = that.args[1];
          var value = store.decr(key);
          reply(":" + value);
        }
      },

      info: {
        inline: true,
        callback: function() {
          debug("received INFO command");
          reply("a5e, the awesome node.js redis clone");
        }
      },

      keys: {
        inline: true,
        callback: function() {
          debug("received KEYS command");
          var pattern = that.args[1];
          var result = store.keys(pattern);
          reply("$" + result.length);
          reply(result);
        }
      },

      mget: {
        inline: true,
        callback: function() {
          debug("received MGET command");
          var keys = that.args.slice(1);
          var values = store.mget(keys);
          reply("*" + values.length);
          values.forEach(function(value) {
            if(value) {
              reply("$" + value.length);
              reply(value);
            } else {
              reply("$-1");
            }
          });
        }
      },

      quit: {
        inline: true,
        callback: function() {
          debug("received QUIT command");
          socket.close();
          server.close();
        }
      },

      select: {
        inline: true,
        callback: function() {
          debug("received SELECT command");
          var index = that.args[1];
          store.select(index);
          socket.send(ok);
        }
      },

      set: {
        inline: false,
        callback: function() {
          debug("received SET command");
          var key = that.args[1];
          store.set(key, that.data);
          socket.send(ok);
        }
      },

      // for debugging
      dump: {
        inline: true,
        callback: function() {
          debug("received DUMP command");
          sys.print(store.dump() + eol);
          socket.send(ok);
        }
      },
    };

    this.is_inline = function() {
      if(typeof callbacks[this.cmd] === "undefined") {
        return true; // unkown cmds are inline
      }

      return callbacks[this.cmd].inline;
    };

    this.setData = function(data) {
      this.data = data.trim();
    }

    this.exec = function() {
      debug("in exec " + this.cmd);
      if(callbacks[this.cmd]) {
        callbacks[this.cmd].callback(this.args);
      } else {
        // ignoring unknown command
      }
    };
    
    return this;
  }

  socket.setEncoding("utf8"); // check with redis protocol

  function debug(s) {
    if(enable_debug) {
      sys.print(s + eol);
    }
  }

  // for reading requests

  function adjustBuffer(buffer) {
    return buffer.substr(buffer.lastIndexOf(eol) + 2)
  }

  function parseData(s) {
    return s.substring(s.indexOf(eol) + 2, s.lastIndexOf(eol));
  }

  var buffer = "";
  var in_bulk_request = false;
  var cmd = {};
  socket.addListener("receive", function(packet) {
    buffer += packet;
    debug("read: '" + buffer.substr(0, 36) + "'");
    var idx;
    if(idx = buffer.indexOf(eol) != -1) { // we have a newline
      if(in_bulk_request) {
        debug("in bulk req");
        // later
        cmd.setData(buffer);
        in_bulk_request = false;
        buffer = adjustBuffer(buffer);
        cmd.exec();
      } else {
        // not a bulk request yet
        debug("not in bulk req yet");
        cmd = Command(buffer);
        if(cmd.is_inline()) {
          cmd.exec();
        } else {
          if(buffer.indexOf(eol) != buffer.lastIndexOf(eol)) { // two new lines
            // parse out command line
            cmd.setData(parseData(buffer));
            in_bulk_request = false;
            cmd.exec();
          } else {
            debug("wait for bulk: '" + buffer + "'");
            in_bulk_request = true;
          }
        }
        buffer = adjustBuffer(buffer);
      }
    }
  });

  socket.addListener("eof", function() {
    socket.close();
  });
});

server.listen(PORT, "localhost");
