# Awesome is a Redis clone in node.js

## WORK IN PROGRESS

At this point, Awesome has all the plumbing in place to implement all of Redis'
commands. It comes with a bunch of commands to show you how to add your own. But it
does not yet implement all that is needed to be a grown up Redis. I hope you can
pitch in and add your favourite Redis commands :)


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

Yay!


## Run Tests

We're lazy, just run the Redis test suite:

    $ cd ../redis
    $ make test
    ...will hang somwhere since Awesome doesn't support all commands yet.

## License

MIT License. See LICENSE file.


## Who?

Initial code by Jan Lehnardt <jan@apache.org>.

Special thanks to Ryan Dahl (ry) for node.js and Salvatore Sanfilippo (antirez)
Redis.
