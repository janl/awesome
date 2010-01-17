/*
  Dump the redis protocol

  Initial code by Jan Lehnardt <jan@apache.org>

  Use:
  # shell one
  $ node redis-cmd-reader.js
  # shell two
  $ redis-client cmd args
  # back in shell one you should see raw the protocl

  MIT License
*/
var PORT = 6379;

var tcp = require("tcp");
var sys = require("sys");

var server = tcp.createServer(function(socket) {
  socket.addListener("receive", function(packet) {
    debug("read: '" + packet + "'");
  });
});

server.listen(PORT, "localhost");

function debug(s) {
  if(s !== null) {
    sys.print(s.toString().substr(0,128) + "\r\n");
  }
}