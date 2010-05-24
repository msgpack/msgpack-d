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

version(unittest) import std.typecons, msgpack.common;


@trusted:


/**
 * Serializes $(D_PARAM args).
 *
 * Single object if the length of $(D_PARAM args) == 1,
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
        packer.pack(args);
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
 * Deserializes $(D_PARAM buffer) using stream deserializer.
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
Unpacked unpack(Tdummy = void)(in ubyte[] buffer)
{
    auto unpacker = unpacker(buffer);

    if (!unpacker.execute())
        throw new UnpackException("Deserialization failure");

    return unpacker.unpacked;
}


/**
 * Deserializes $(D_PARAM buffer) using direct conversion deserializer.
 *
 * Single object if the length of $(D_PARAM args) == 1,
 * otherwise array object.
 *
 * Params:
 *  buffer = the buffer to deserialize.
 *  args   = the references of values to assign.
 */
void unpack(Args...)(in ubyte[] buffer, ref Args args)
{
    auto unpacker = unpacker!(false)(buffer);

    static if (Args.length == 1) {
        unpacker.unpack(args[0]);
    } else {
        unpacker.unpackArray();
        unpacker.unpack(args);
    }
}


unittest
{
    { // stream
        auto result = unpack(pack(false));

        assert(result.via.boolean == false);
    }
    { // direct conversion
        Tuple!(uint, string) result, test = tuple(1, "Hi!");
        
        unpack(pack(test), result);

        assert(result == test);

        test.field[0] = 2;
        test.field[1] = "Hey!";

        unpack(pack(test.field[0], test.field[1]), result.field[0], result.field[1]);

        assert(result == test);
    }
}
