/*
  amazing is a redis clone for node.js

  Initial code by Jan Lehnardt <jan@apache.org>

  MIT License
*/

var tcp = require("tcp");
var sys = require("sys");


var debug = true;

var store = require("./store");

var server = tcp.createServer(function(socket) {
  // requests and responses have this as a trailer
  var eol = "\r\n";
  var ok = "+OK" + eol;
  // our storage object. All data goes here

  function Command(line) {

    function parseCommand(s) {
      debug("in parseCommand");
      debug("'" + s + "'");
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
      var arg;
      var argidx = 0;
      for(var idx = 0; idx < s.length; ++idx) {
        var chr = s[idx];
        if(chr == " " || chr == "\r" || chr == "\n") {
          args.push(arg);
          argidx = argidx + 1;
        } else {
          arg += chr;
        }
      }
      return args;
    }

    this.cmd = parseCommand(line).toLowerCase();
    this.args = parseArgs(line);

    debug("got command '" + this.cmd + "'");
    that = this;

    var callbacks = {
      get: {
        inline: true,
        callback: function() {
          debug("received GET command");
          var key = that.args[1];
          if(store.has(key)) {
            var value = store.get(key);
            socket.send("$" + value.length + eol);
            socket.send(value + eol);
          } else { // not found
            socket.send("$-1" + eol);
          }
        }
      },

      getset: {
        inline: false,
        callback: function() {
          debug("received GETSET comand");
          var key = that.args[1];
          if(store.has(key)) {
            var value = store.get(key);
            socket.send("$" + value.length + eol);
            socket.send(value + eol);
          } else { // not found
            socket.send("-1" + eol);
          }
        }
      },

      info: {
        inline: true,
        callback: function() {
          socket.send("a5e, the awesome node.js redis clone");
        }
      },

      dbsize: {
        inline: true,
        callback: function() {
          debug("received DBSIZE command");
          var size = store.dbsize();
          socket.send(":" + size + eol);
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
      debug("out exec");
    };
    
    return this;
  }

  socket.setEncoding("utf8"); // check with redis protocol

  function debug(s) {
    if(debug) {
      sys.print(s + eol);
    }
  }

  // for reading requests

  function adjustBuffer(buffer) {
    return buffer.substr(buffer.lastIndexOf(eol) + 2)
  }

  var buffer = "";
  var in_bulk_request = false;
  var cmd = {};
  socket.addListener("receive", function(packet) {
    buffer += packet;
    debug(buffer);
    var idx;
    if(idx = buffer.indexOf(eol) != -1) { // we have a newline
      if(in_bulk_request) {
        // later
        cmd.setData(buffer);
        cmd.exec();
        in_bulk_request = false;
        buffer = adjustBuffer(buffer);
      } else {
        // not a bulk request yet
        cmd = Command(buffer);
        buffer = adjustBuffer(buffer);
        if(cmd.is_inline) {
          cmd.exec();
        } else {
          in_bulk_request = true;
        }
      }
    }
  });

  socket.addListener("eof", function() {
    socket.close();
  });
});

server.listen(6379, "localhost");
