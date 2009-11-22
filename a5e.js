/*
  a5e, or "amazing", is a redis clone for node.js

  Initial code by Jan Lehnardt <jan@apache.org>

  MIT License
*/

var tcp = require("tcp");
var sys = require("sys");


var debug = true;

function parseCommand(s) {
  var regex = /([A-Z]+) ?([^ ])? ?([^ ])?\s+$/;
  //            cmd       arg0     arg1

  var parsed_command = s.match(regex);
  if(!parsed_command) {
    throw("unkown command");
  }
  var result = {};
  result.cmd = parsed_command[1];
  
  if(parsed_command.length > 1) {
    result.args = parsed_command.slice(1);
  }
  return result;
}

var server = tcp.createServer(function(socket) {
  // requests and responses have this as a trailer
  var eol = "\r\n";
  var ok = "+OK" + eol;
  // our storage object. All data goes here
  var store = {};

  function Command(line) {
    this.line = parseCommand(line);
    this.cmd = this.line.cmd;
    this.args = this.line.args;

    var bulk_commands = ["SET"];
    that = this;

    var callbacks = {
      QUIT: {
        inline: true,
        callback: function() {
          debug("received QUIT command");
          socket.close();
          server.close();
        }
      },

      SET: {
        inline: false,
        callback: function() {
          debug("received SET command");
          store[that.args[1]] = that.data;
          socket.send(ok);
        }
      },

      GET: {
        inline: true,
        callback: function() {
          debug("received GET command");
          var key = that.args[1];
          if(store[key]) {
            socket.send("$" + store[key].length + eol);
            socket.send(store[key] + eol);
          } else { // not found
            socket.send("$-1" + eol);
          }
        }
      },

      INFO: {
        inline: true,
        callback: function() {
          socket.send("a5e, the awesome node.js redis clone");
        }
      },

      // for debugging
      DUMP: {
        inline: true,
        callback: function() {
          debug("received DUMP command");
          sys.print(JSON.stringify(store) + eol);
          socket.send(ok);
        }
      }
    };

    this.is_inline = function() {
      return callbacks[this.cmd] && callbacks[this.cmd].inline;
    };

    this.setData = function(data) {
      this.data = data;
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
  String.prototype.occurs = function(needle) {
    debug("in occurs");
    var count = 0;
    var idx = 0;
    while((idx = this.indexOf(needle, idx + (idx > 0 ? 1 : 0))) != -1) {
      count = count + 1;
    }
    debug("out occurs");
    return count;
  }

  // we don't need no connect handler

  function debug(s) {
    if(debug) {
      sys.print(s + eol);
    }
  }

  // for reading requests

  function adjustBuffer(buffer) {
    return buffer.substr(buffer.lastIndexOf(eol) + 1)
  }
  var buffer = "";
  var in_bulk_request = false;
  var cmd = {};
  socket.addListener("receive", function(packet) {
    buffer += packet;
    debug(buffer);
    switch(buffer.occurs(eol)) {
      case 0: return; // no newlines yet, keep buffering
      case 1:
        debug("one newline");
        // one newline
        if(!in_bulk_request) {
          debug("not in bulk req")
          cmd = Command(buffer);
          if(cmd && cmd.is_inline()) {
            cmd.exec();
            // adjust buffer
            buffer = adjustBuffer(buffer);
          } else {
            debug("set in bulk req true");
            in_bulk_request = true;
          }
        } /* else { // we're in a bulk request
          // keep buffering until we have two newlines
        } */
      break;
      case 2:
        debug("two newlines");
        if(!in_bulk_request) {
          // err
        }
        cmd.setData(buffer.substring(buffer.indexOf(eol) + 2, buffer.lastIndexOf(eol)));
        cmd.exec();
        // adjust buffer
        buffer = adjustBuffer(buffer);
        in_bulk_request = false;
      break;
      default:
        // wtf?
        // throw error
    }
  });

  socket.addListener("eof", function() {
    socket.close();
  });
});

server.listen(6379, "localhost");
