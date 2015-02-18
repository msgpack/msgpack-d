// Written in the D programming language.

/**
 * Serializer and Stream Deserializer usage
 */

import std.array;
import std.stdio;

import msgpack;


void main()
{
    auto packer = packer(appender!(ubyte[])());

    int[] emptyArray;
    int[int] emptyMap;
    packer.packArray(null, true, "Hi!", -1, [1, 2], emptyArray, emptyMap);

    auto unpacker = StreamingUnpacker(packer.stream.data);

    if (unpacker.execute()) {
        foreach (obj; unpacker.purge())
            writeln(obj.type);
    } else {
        writeln("Serialized object is too large!");
    }
}
