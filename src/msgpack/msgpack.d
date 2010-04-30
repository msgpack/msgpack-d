// Written in the D programming language.

/**
 * MessagePack for D, convenient functions
 *
 * Copyright: Copyright Masahiro Nakagawa 2010.
 * License:   <a href = "http://www.boost.org/LICENSE_1_0.txt">Boost License 1.0</a>.
 * Authors:   Masahiro Nakagawa
 */
module msgpack.msgpack;

public import msgpack.object;

import msgpack.buffer;
import msgpack.packer;
import msgpack.unpacker;

version(unittest) import msgpack.common;


@trusted:


/**
 * Serializes $(D_PARAM args).
 *
 * Single object if $(D_PARAM args) lenght equals 1,
 * otherwise array object.
 *
 * Params:
 *  args = the contents to serialize.
 *
 * Returns:
 *  a serialized data.
 */
ubyte[] pack(Args...)(in Args args)
{
    SimpleBuffer buffer;
    auto packer = packer(&buffer);

    static if (Args.length == 1) {
        packer.pack(args[0]);
    } else {
        packer.packArray(Args.length);
        foreach (arg; args)
            packer.pack(arg);
    }

    return packer.buffer.data;
}


unittest
{
    auto serialized = pack(false);

    assert(serialized[0] == Format.FALSE);

    auto deserialized = unpack(pack(1, true, "Foo"));

    assert(deserialized.type == mp_Type.ARRAY);
    assert(deserialized.via.array[0].type == mp_Type.POSITIVE_INTEGER);
    assert(deserialized.via.array[1].type == mp_Type.BOOLEAN);
    assert(deserialized.via.array[2].type == mp_Type.RAW);
}


/**
 * Deserializes $(D_PARAM buffer).
 *
 * Params:
 *  buffer = the buffer to deserialize.
 *
 * Returns:
 *  a $(D Unpacked) contains deserialized object.
 *
 * Throws:
 *  UnpackException if deserialization doesn't succeed.
 */
Unpacked unpack(in ubyte[] buffer)
{
    auto unpacker = unpacker(buffer);

    if (!unpacker.execute())
        throw new UnpackException("Deserialization failure");

    return unpacker.unpacked;
}


unittest
{
    auto result = unpack(pack(false));

    assert(result.via.boolean == false);
}
