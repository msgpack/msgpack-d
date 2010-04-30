// Written in the D programming language.

/**
 * Stream deserialization usage
 *
 * This example works Windows. In Max OS X, std.concurrency doesn't work.
 */

import std.array;
import std.concurrency;
import std.stdio;

import msgpack.msgpack;
import msgpack.unpacker;


void deserializer(Tid tid)
{
    auto unpacker = unpacker(null);
    bool endLoop;

    while (true) {
        receive((ubyte[] data) { unpacker.append(data); },
                (bool    end)  { endLoop = end; });

        if (endLoop)
            break;

        while (unpacker.execute()) {
            auto unpacked = unpacker.purge();
            writeln("Type:  ", unpacked.type);
            writeln("Value: ", unpacked.as!(string));
        }

        if (unpacker.size >= 100)
            throw new Exception("Too large!");
    }
}


void main()
{
    string message = "Hell";
    foreach (i; 0..93)  // Throws Exception if 94
        message ~= 'o';

    auto packed = pack(message);
    auto tid    = spawn(&deserializer, thisTid);

    while (!packed.empty) {
        auto limit = packed.length >= 10 ? 10 : packed.length;

        send(tid, packed[0..limit]);
        packed  = packed[limit..$];
    }

    send(tid, true);
}
