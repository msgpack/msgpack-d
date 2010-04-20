// Written in the D programming language.

/**
 * Serializer and Deserializer usage
 */

import std.stdio;

import msgpack.buffer;
import msgpack.packer;
import msgpack.unpacker;


void main()
{
    SimpleBuffer buffer;
    auto packer = packer(&buffer);

    packer.packArray(5).packNil().packTrue().pack("Hi!").pack(-1).pack([1, 2]);

    auto unpacker = unpacker(packer.buffer.data);

    if (unpacker.execute()) {
        foreach (obj; unpacker)
            writeln(obj.type);
    } else {
        writeln("Serialized object is too large!");
    }
}
