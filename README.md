# Awesome is a Redis clone in node.js

## WORK IN PROGRESS

At this point, Awesome has all the plumbing in place to implement all of Redis'
commands. It comes with a bunch of commands to show you how to add your own.

## What?

Awesome aims to be a drop-in replacement for Redis in a node.js environment. Awesome
doesn't claim to better, faster, smaller, whatever.

If nothing else, this code helps me understand Redis and node.js. I hope it helps
others to learn either or both, too.

## Run Awesome

    $ node awesome.js
    ...bunch of debugging crap...

On another terminal

    $ telnet localhost 6379
    SET a 3
    foo
    +OK
    GET a
    $3
    foo
    QUIT


## Licenase

MIT License. See LICENSE file.


## Who?

Initial code by Jan Lehnardt <jan@apache.org>.