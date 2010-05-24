// Written in the D programming language.

/**
 * MessagePack for D, common and system dependent operation
 *
 * Copyright: Copyright Masahiro Nakagawa 2010.
 * License:   <a href = "http://www.boost.org/LICENSE_1_0.txt">Boost License 1.0</a>.
 * Authors:   Masahiro Nakagawa
 */
module msgpack.common;

import std.traits;
import std.typetuple;

// for Converting Endian using ntohs and ntohl;
version (Windows)
{
    import std.c.windows.winsock;
}
else
{
    import core.sys.posix.arpa.inet;
}


@trusted:
package:


/**
 * MessagePack type-information format
 *
 * See_Also:
 *  $(LINK2 http://msgpack.sourceforge.net/spec, MessagePack Specificaton)
 */
enum Format : ubyte
{
    // unsinged integer
    UINT8  = 0xcc,  // ubyte
    UINT16 = 0xcd,  // ushort
    UINT32 = 0xce,  // uint
    UINT64 = 0xcf,  // ulong

    // signed integer
    INT8  = 0xd0,   // byte
    INT16 = 0xd1,   // short
    INT32 = 0xd2,   // int
    INT64 = 0xd3,   // long

    // floating point
    FLOAT  = 0xca,  // float
    DOUBLE = 0xcb,  // double

    // raw byte
    RAW   = 0xa0,
    RAW16 = 0xda,
    RAW32 = 0xdb,

    // array
    ARRAY   = 0x90,
    ARRAY16 = 0xdc,
    ARRAY32 = 0xdd,

    // map
    MAP   = 0x80,
    MAP16 = 0xde,
    MAP32 = 0xdf,

    // other
    NIL   = 0xc0,   // null
    TRUE  = 0xc3,
    FALSE = 0xc2,

    // real (This format is D only!)
    REAL = 0xd4
}


/**
 * For float type serialization / deserialization
 */
union _f
{
    float f;
    uint  i;
}


/**
 * For double type serialization / deserialization
 */
union _d
{
    double f;
    ulong  i;
}


static if (real.sizeof == 16) {
    /**
     * For real type serialization / deserialization on 128bit environment
     */
    union _r
    {
        real f;

        struct
        {
            ulong fraction;
            ulong exponent;  // includes sign
        }
    }

    enum ES = ulong.sizeof * 8;  // exponent size as bits
} else static if (real.sizeof == 12) {
    /**
     * For real type serialization / deserialization on 96bit environment
     */
    union _r
    {
        real f;

        struct
        {
            ulong fraction;
            uint  exponent;  // includes sign
        }
    }

    enum ES = uint.sizeof * 8;  // exponent size as bits
} else {
    /**
     * For real type serialization / deserialization on 80bit environment
     */
    union _r
    {
        real f;

        struct
        {
            ulong  fraction;
            ushort exponent;  // includes sign
        }
    }

    enum ES = ushort.sizeof * 8;  // exponent size as bits
}


/**
 * Detects whether $(D_PARAM T) is a built-in byte type.
 */
template isByte(T)
{
    enum isByte = staticIndexOf!(Unqual!T, byte, ubyte) >= 0;
}


unittest
{
    static assert(isByte!(byte));
    static assert(isByte!(const(byte)));
    static assert(isByte!(ubyte));
    static assert(isByte!(immutable(ubyte)));
    static assert(!isByte!(short));
    static assert(!isByte!(char));
    static assert(!isByte!(string));
}


version (LittleEndian)
{
    /**
     * Converts $(value) to different Endian.
     *
     * Params:
     *  value = the LittleEndian value to convert.
     *
     * Returns:
     *  the converted value.
     */
    ushort convertEndianTo(size_t Bit, T)(in T value) if (Bit == 16)
    {
        return ntohs(cast(ushort)value);
    }


    /// ditto
    uint convertEndianTo(size_t Bit, T)(in T value) if (Bit == 32)
    {
        return ntohl(cast(uint)value);
    }


    /// ditto
    ulong convertEndianTo(size_t Bit, T)(in T value) if (Bit == 64)
    {
        // dmd has convert function?
        return ((((cast(ulong)value) << 56) & 0xff00000000000000UL) |
                (((cast(ulong)value) << 40) & 0x00ff000000000000UL) |
                (((cast(ulong)value) << 24) & 0x0000ff0000000000UL) |
                (((cast(ulong)value) <<  8) & 0x000000ff00000000UL) |
                (((cast(ulong)value) >>  8) & 0x00000000ff000000UL) |
                (((cast(ulong)value) >> 24) & 0x0000000000ff0000UL) |
                (((cast(ulong)value) >> 40) & 0x000000000000ff00UL) |
                (((cast(ulong)value) >> 56) & 0x00000000000000ffUL));
    }


    unittest
    {
        assert(convertEndianTo!16(0x0123)             == 0x2301);
        assert(convertEndianTo!32(0x01234567)         == 0x67452301);
        assert(convertEndianTo!64(0x0123456789abcdef) == 0xefcdab8967452301);
    }


    /**
     * Comapatible for BigEndian environment.
     */
    ubyte take8from(size_t bit = 8, T)(T value)
    {
        static if (bit == 8 || bit == 16 || bit == 32 || bit == 64)
            return (cast(ubyte*)&value)[0];
        else
            static assert(false, bit.stringof ~ " is not support bit width.");
    }


    unittest
    {
        foreach (Integer; TypeTuple!(ubyte, ushort, uint, ulong)) {
            assert(take8from!8 (cast(Integer)0x01)               == 0x01);
            assert(take8from!16(cast(Integer)0x0123)             == 0x23);
            assert(take8from!32(cast(Integer)0x01234567)         == 0x67);
            assert(take8from!64(cast(Integer)0x0123456789abcdef) == 0xef);
        }
    }
}
else
{
    /**
     * Comapatible for LittleEndian environment.
     */
    ushort convertEndianTo(size_t Bit, T)(in T value) if (Bit == 16)
    {
        return cast(ushort)value;
    }


    /// ditto
    uint convertEndianTo(size_t Bit, T)(in T value) if (Bit == 32)
    {
        return cast(uint)value;
    }


    /// ditto
    ulong convertEndianTo(size_t Bit, T)(in T value) if (Bit == 64)
    {
        return cast(ulong)value;
    }


    unittest
    {
        assert(convertEndianTo!16(0x0123)       == 0x0123);
        assert(convertEndianTo!32(0x01234567)   == 0x01234567);
        assert(convertEndianTo!64(0x0123456789) == 0x0123456789);
    }


    /**
     * Takes 8bit from $(D_PARAM value)
     *
     * Params:
     *  value = the content to take.
     *
     * Returns:
     *  the 8bit value corresponding $(D_PARAM bit) width.
     */
    ubyte take8from(size_t bit = 8, T)(T value)
    {
        static if (bit == 8)
            return (cast(ubyte*)&value)[0];
        else static if (bit == 16)
            return (cast(ubyte*)&value)[1];
        else static if (bit == 32)
            return (cast(ubyte*)&value)[3];
        else static if (bit == 64)
            return (cast(ubyte*)&value)[7];
        else
            static assert(false, bit.stringof ~ " is not support bit width.");
    }


    unittest
    {
        foreach (Integer; TypeTuple!(ubyte, ushort, uint, ulong)) {
            assert(take8from!8 (cast(Integer)0x01)               == 0x01);
            assert(take8from!16(cast(Integer)0x0123)             == 0x23);
            assert(take8from!32(cast(Integer)0x01234567)         == 0x67);
            assert(take8from!64(cast(Integer)0x0123456789abcdef) == 0xef);
        }
    }
}


/**
 * Loads $(D_PARAM T) type value from $(D_PARAM buffer).
 *
 * Params:
 *  buffer = the serialized contents.
 *
 * Returns:
 *  the Endian-converted value.
 */
T load16To(T)(ubyte[] buffer)
{
    return cast(T)(convertEndianTo!16(*cast(ushort*)buffer.ptr));
}


/// ditto
T load32To(T)(ubyte[] buffer)
{
    return cast(T)(convertEndianTo!32(*cast(uint*)buffer.ptr));
}


/// ditto
T load64To(T)(ubyte[] buffer)
{
    return cast(T)(convertEndianTo!64(*cast(ulong*)buffer.ptr));
}
