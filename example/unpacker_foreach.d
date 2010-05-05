// Written in the D programming language.

/**
 * Stream Deserializer with foreach.
 */

import std.stdio;

import msgpack.msgpack;
import msgpack.unpacker;


void main()
{
    // create 3 MessagePack objects([1, 0.1L], true, "foobarbaz")
    auto test1 = pack(1, 0.1L) ~ pack(true);
    auto test2 = pack("foobarbaz");

    // split data to deserialize test
    test1 ~= test2[0..2];
    test2  = test2[2..$];

    auto unpacker = unpacker(test1);

    foreach (unpacked; unpacker) {
        if (unpacked.type == mp_Type.ARRAY) {
            foreach (obj; unpacked) {
                switch (obj.type) {
                case mp_Type.POSITIVE_INTEGER: writeln(obj.as!(uint)); break;
                case mp_Type.FLOAT:            writeln(obj.as!(real)); break;
                defalut:
                    throw new Exception("Unknown type");
                }
            }
        } else {
            writeln(unpacked.as!(bool));
        }
    }

    unpacker.append(test2);

    foreach (unpacked; unpacker)
        writeln(unpacked.as!(string));
}
