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


/**
 * Serializes $(D_PARAM value).
 *
 * Params:
 *  value = the content to serialize.
 *
 * Returns:
 *  a serialized data.
 */
ubyte[] pack(T)(in T value)
{
    SimpleBuffer buffer;
    auto packer = packer(&buffer);

    packer.pack(value);

    return packer.buffer.data;
}


unittest
{
    auto result = pack(false);

    assert(result[0] == Format.FALSE);
}


/**
 * Deserializes $(D_PARAM buffer).
 *
 * Params:
 *  buffer = the buffer to deserialize.
 *
 * Returns:
 *  a deserialized object.
 *
 * Throws:
 *  UnpackException if deserialization doesn't succeed.
 */
mp_Object unpack(in ubyte[] buffer)
{
    auto unpacker = unpacker(buffer);

    if (!unpacker.execute())
        throw new UnpackException("Deserialization failure");

    return unpacker.data;
}


unittest
{
    auto result = unpack(pack(false));

    assert(result.via.boolean == false);
}
