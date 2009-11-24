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
  
  var EMPTY_VALUE = {};

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
      debug('reply: ' + line); 
      socket.send(line + eol);
    }
    
    function replyString(s) {
      reply("$" + s.length);
      reply(s);
    }

    this.cmd = parseCommand(line).toLowerCase();
    this.args = parseArgs(line);

    var that = this;

    var callbacks = {
      // keep sorted alphabetically
      // list-related functions at the end
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
            if (EMPTY_VALUE === value) {
              // empty value
              reply("$0");
              reply("");
            } else {
              reply("$" + value.toString().length);
              reply(value);
            }
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

      incrby: {
        inline: true,
        callback: function() {
          var key = that.args[1];
          var increment = that.args[2];
          var value = store.incrby(key, increment);
          reply(":" + value);
        }
      },

      decrby: {
        inline: true,
        callback: function() {
          var key = that.args[1];
          var decrement = that.args[2];
          var value = store.decrby(key, decrement);
          reply(":" + value);
        }
      },

      exists: {
        inline: true,
        callback: function() {
          debug("received EXISTS command");
          var key = that.args[1];
          if(store.has(key)) {
            reply(":1");
          } else {
            reply(":0");
          }
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
          var pattern = that.args[1] || '*';
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

      ping: {
        inline: true,
        callback: function() {
          debug("received PING");
          reply("+PONG");
        }
      },

      quit: {
        inline: true,
        callback: function() {
          debug("received QUIT command");
          socket.close();
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
          var value = that.data || EMPTY_VALUE;
          store.set(key, value);
          socket.send(ok);
        }
      },

      setnx: {
        inline: false,
        callback: function() {
          debug("received SETNX command");
          var key = that.args[1];
          if(!store.has(key)) {
            store.set(key, that.data);
            reply(":1");
          } else {
            reply(":0");
          }
        }
      },
      
      // list related functions
      lindex : {
        inline: true,
        callback: function() {
          debug("received LINDEX command");
          var key = that.args[1];
          var index = that.args[2];
          debug('index = ' +index);
          
          if (index && store.has(key)) {
            var arr = store.get(key);
            if (index < 0) {
              index += arr.length;
            }
            if (index < 0 || index > arr.length) {
              replyString('');
            } else {
              replyString(arr[index]);
            }
          } else {
            // FEHLER
            socket.send('-index not a number'+eol);
          }
        }
      },
      llen : {
        inline: true,
        callback: function() {
          debug("received LLEN command");
          var key = that.args[1];
          if(store.has(key)) {
            reply(":" + store.get(key).length);
          } else {
            // FEHLER
          }
        }
      },
      lpush : {
        inline: false,
        callback: function() {
          debug("received LPUSH command");
          var key = that.args[1];
          var value = that.data || EMPTY_VALUE;
          if(!store.has(key)) {
            store.set(key, [value]);
          } else {
            store.get(key).unshift(value);
          }
          socket.send(ok);
        }
      },
      lpop : {
        inline: true,
        callback: function() {
          debug("received LPOP command");
          var key = that.args[1];
          if(store.has(key)) {
            reply(store.get(key).shift());
          } else {
            // FEHLER
          }
        }
      },
      rpush : {
        inline: false,
        callback: function() {
          debug("received RPUSH command");
          var key = that.args[1];
          var value = that.data || EMPTY_VALUE;
          if(!store.has(key)) {
            store.set(key, [value]);
          } else {
            store.get(key).push(value);
          }
          socket.send(ok);
        }
      },
      rpop : {
        inline: false,
        callback: function() {
          debug("received RPOP command");
          var key = that.args[1];
          if(store.has(key)) {
            reply(store.get(key).pop());
          } else {
            // FEHLER
          }
          
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
      
      foobaredcommand: {
        inline: true,
        callback: function() {
          socket.send('-unknown function'+eol);
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
      sys.print(s.substr(0,40) + eol);
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

  var buffer = "";
  var in_bulk_request = false;
  var cmd = {};
  socket.addListener("receive", function(packet) {
    buffer += packet;
    debug("read: '" + buffer.substr(0, 36) + "'");
    var idx;
    while (idx = buffer.indexOf(eol) != -1) { // we have a newline
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
    
    // debug('buffer:' + buffer);
    
  });

  socket.addListener("eof", function() {
    socket.close();
  });
});

server.listen(PORT, "localhost");
