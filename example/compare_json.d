// Written in the D programming language.

/**
 * Compares std.json
 */

import std.date;
import std.json;
import std.stdio;

import msgpack.msgpack;


void main()
{
    JSONValue jsonObj = parseJSON(`[12, "foo", true, 0.23]`);

    void f1()
    {
        parseJSON(toJSON(&jsonObj));
    }

    mp_Object mpObj = mp_Object([mp_Object(12UL), mp_Object(cast(ubyte[])"foo"), mp_Object(true), mp_Object(0.23)]);

    void f2()
    {
        unpack(pack(mpObj));
    }

    auto times = benchmark!(f1, f2)(10000);
    writeln("JSON:    ", times[0]);
    writeln("Msgpack: ", times[1]);
}
