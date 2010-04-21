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
    JSONValue jsonObj;
    jsonObj.array = [parseJSON(`[12, "foo", true, 0.23]`)];
    jsonObj.type  = JSON_TYPE.ARRAY;

    void f1()
    {
        parseJSON(toJSON(&jsonObj));
    }

    mp_Object mpObj;
    mpObj = mp_Object([mp_Object(12UL), mp_Object(cast(ubyte[])"foo"), mp_Object(true), mp_Object(0.23)]);

    void f2()
    {
        unpack(pack(mpObj));
    }

    auto results = benchmark!(f1, f2)(10000);
    writeln("JSON:    ", results[0]);
    writeln("Msgpack: ", results[1]);
}
