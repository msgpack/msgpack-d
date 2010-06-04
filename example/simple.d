// Written in the D programming language.

/**
 * Serializer and Stream Deserializer usage
 */

import std.stdio;

import msgpack.buffer;
import msgpack.packer;
import msgpack.unpacker;


void main()
{
    auto packer = packer(SimpleBuffer());

    packer.packArray(5).packNil().packTrue().pack("Hi!", -1, [1, 2]);

    auto unpacker = unpacker(packer.buffer.data);

    if (unpacker.execute()) {
        foreach (obj; unpacker.purge())
            writeln(obj.type);
    } else {
        writeln("Serialized object is too large!");
    }
}
