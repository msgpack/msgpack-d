// Written in the D programming language.

/**
 * MessagePack for D, serializing routine
 *
 * Copyright: Copyright Masahiro Nakagawa 2010.
 * License:   <a href = "http://www.boost.org/LICENSE_1_0.txt">Boost License 1.0</a>.
 * Authors:   Masahiro Nakagawa
 */
module msgpack.packer;

import msgpack.common;

import std.traits;
import std.typetuple;


/**
 * $(D isWritableBuffer) is a constraint for buffer that $(D Packer) uses.
 *
 * Returns true if $(D_PARAM Buffer) is an byte buffer that defines write method.
 * The following code is a concept example.
-----
Buffer  buffer;
ubyte[] values = [1];

buffer.write(values[0]);
buffer.write(values);
-----
 */
template isWritableBuffer(Buffer)
{
    // which is better __traits or is(typeof({}()))?
    enum bool isWritableBuffer = __traits(compiles,
    {
        Buffer  buffer;
        ubyte[] values = [1];
        buffer.write(values[0]);
        buffer.write(values);
    });
}


/**
 * $(D Packer) is a $(D MessagePack) serializer.
 */
struct Packer(Buffer) if (isWritableBuffer!(Buffer))
{
  private:
    alias .Packer!(Buffer) Packer;

    enum size_t Offset = 1;  // type-information offset

    Buffer   buffer_;  // the buffer to write
    ubyte[9] store_;   // stores serialized value


  public:
    /**
     * Constructs a packer with $(D_PARAM buffer).
     *
     * Params:
     *  buffer = the buffer to write.
     */
    this(Buffer buffer)
    {
        buffer_ = buffer;
    }


    /**
     * Forwards to buffer.
     *
     * Returns:
     *  the buffer.
     */
    @property Buffer buffer()
    {
        return buffer_;
    }


    /**
     * Serializes $(D_PARAM value) and writes to buffer.
     *
     * Params:
     *  value = the content to serialize.
     *
     * Returns:
     *  this to method chain.
     */
    Packer pack(T : bool)(in T value)
    {
        return value ? packTrue() : packFalse();
    }


    /// ditto
    Packer pack(T : ubyte)(in T value)
    {
        if (value < (1 << 7)) {
            // fixnum
            buffer_.write(take8from(value));
        } else {
            // uint 8
            store_[0] = Format.UINT8;
            store_[1] = take8from(value);
            buffer_.write(store_[0..Offset + ubyte.sizeof]);
        }

        return this;
    }


    /// ditto
    Packer pack(T : ushort)(in T value)
    {
        if (value < (1 << 7)) {
            // fixnum
            buffer_.write(take8from!16(value));
        } else if (value < (1 << 8)) {
            // uint 8
            store_[0] = Format.UINT8;
            store_[1] = take8from!16(value);
            buffer_.write(store_[0..Offset + ubyte.sizeof]);
        } else {
            // uint 16
            const temp = convertEndianTo!16(value);

            store_[0] = Format.UINT16;
            *cast(ushort*)&store_[Offset] = temp;
            buffer_.write(store_[0..Offset + ushort.sizeof]);
        }

        return this;
    }


    /// ditto
    Packer pack(T : uint)(in T value)
    {
        if (value < (1 << 8)) {
            if (value < (1 << 7)) {
                // fixnum
                buffer_.write(take8from!32(value));
            } else {
                // uint 8
                store_[0] = Format.UINT8;
                store_[1] = take8from!32(value);
                buffer_.write(store_[0..Offset + ubyte.sizeof]);
            }
        } else {
            if (value < (1 << 16)) {
                // uint 16
                const temp = convertEndianTo!16(value);

                store_[0] = Format.UINT16;
                *cast(ushort*)&store_[Offset] = temp;
                buffer_.write(store_[0..Offset + ushort.sizeof]);
            } else {
                // uint 32
                const temp = convertEndianTo!32(value);

                store_[0] = Format.UINT32;
                *cast(uint*)&store_[Offset] = temp;
                buffer_.write(store_[0..Offset + uint.sizeof]);
            }
        }

        return this;
    }


    /// ditto
    Packer pack(T : ulong)(in T value)
    {
        if (value < (1UL << 8)) {
            if (value < (1UL << 7)) {
                // fixnum
                buffer_.write(take8from!64(value));
            } else {
                // uint 8
                store_[0] = Format.UINT8;
                store_[1] = take8from!64(value);
                buffer_.write(store_[0..Offset + ubyte.sizeof]);
            }
        } else {
            if (value < (1UL << 16)) {
                // uint 16
                const temp = convertEndianTo!16(value);

                store_[0] = Format.UINT16;
                *cast(ushort*)&store_[Offset] = temp;
                buffer_.write(store_[0..Offset + ushort.sizeof]);
            } else if (value < (1UL << 32)){
                // uint 32
                const temp = convertEndianTo!32(value);

                store_[0] = Format.UINT32;
                *cast(uint*)&store_[Offset] = temp;
                buffer_.write(store_[0..Offset + uint.sizeof]);
            } else {
                // uint 64
                const temp = convertEndianTo!64(value);

                store_[0] = Format.UINT64;
                *cast(ulong*)&store_[Offset] = temp;
                buffer_.write(store_[0..Offset + ulong.sizeof]);
            }
        }

        return this;
    }


    /// ditto
    Packer pack(T : byte)(in T value)
    {
        if (value < -(1 << 5)) {
            // int 8
            store_[0] = Format.INT8;
            store_[1] = take8from(value);
            buffer_.write(store_[0..Offset + byte.sizeof]);
        } else {
            // fixnum
            buffer_.write(take8from(value));
        }

        return this;
    }


    /// ditto
    Packer pack(T : short)(in T value)
    {
        if (value < -(1 << 5)) {
            if (value < -(1 << 7)) {
                // int 16
                const temp = convertEndianTo!16(value);

                store_[0] = Format.INT16;
                *cast(short*)&store_[Offset] = temp;
                buffer_.write(store_[0..Offset + short.sizeof]);
            } else {
                // int 8
                store_[0] = Format.INT8;
                store_[1] = take8from!16(value);
                buffer_.write(store_[0..Offset + byte.sizeof]);
            }
        } else if (value < (1 << 7)) {
            // fixnum
            buffer_.write(take8from!16(value));
        } else {
            if (value < (1 << 8)) {
                // uint 8
                store_[0] = Format.UINT8;
                store_[1] = take8from!16(value);
                buffer_.write(store_[0..Offset + ubyte.sizeof]);                
            } else {
                // uint 16
                const temp = convertEndianTo!16(value);

                store_[0] = Format.UINT16;
                *cast(ushort*)&store_[Offset] = temp;
                buffer_.write(store_[0..Offset + ushort.sizeof]);
            }
        }

        return this;
    }


    /// ditto
    Packer pack(T : int)(in T value)
    {
        if (value < -(1 << 5)) {
            if (value < -(1 << 15)) {
                // int 32
                const temp = convertEndianTo!32(value);

                store_[0] = Format.INT32;
                *cast(int*)&store_[Offset] = temp;
                buffer_.write(store_[0..Offset + int.sizeof]);
            } else if ( -(1 << 7)) {
                // int 16
                const temp = convertEndianTo!16(value);

                store_[0] = Format.INT16;
                *cast(short*)&store_[Offset] = temp;
                buffer_.write(store_[0..Offset + short.sizeof]);
            } else {
                // int 8
                store_[0] = Format.INT8;
                store_[1] = take8from!32(value);
                buffer_.write(store_[0..Offset + byte.sizeof]);
            }
        } else if (value < (1 << 7)) {
            // fixnum
            buffer_.write(take8from!32(value));
        } else {
            if (value < (1 << 8)) {
                // uint 8
                store_[0] = Format.UINT8;
                store_[1] = take8from!32(value);
                buffer_.write(store_[0..Offset + ubyte.sizeof]);
            } else if (value < (1 << 16)) {
                // uint 16
                const temp = convertEndianTo!16(value);

                store_[0] = Format.UINT16;
                *cast(ushort*)&store_[Offset] = temp;
                buffer_.write(store_[0..Offset + ushort.sizeof]);
            } else {
                // uint 32
                const temp = convertEndianTo!32(value);

                store_[0] = Format.UINT32;
                *cast(uint*)&store_[Offset] = temp;
                buffer_.write(store_[0..Offset + uint.sizeof]);
            }
        }

        return this;
    }


    /// ditto
    Packer pack(T : long)(in T value)
    {
        if (value < -(1L << 5)) {
            if (value < -(1L << 15)) {
                if (value < -(1L << 31)) {
                    // int 64
                    const temp = convertEndianTo!64(value);

                    store_[0] = Format.INT64;
                    *cast(long*)&store_[Offset] = temp;
                    buffer_.write(store_[0..Offset + long.sizeof]);
                } else {
                    // int 32
                    const temp = convertEndianTo!32(value);

                    store_[0] = Format.INT32;
                    *cast(int*)&store_[Offset] = temp;
                    buffer_.write(store_[0..Offset + int.sizeof]);
                }
            } else {
                if (value < -(1L << 7)) {
                    // int 16
                    const temp = convertEndianTo!16(value);

                    store_[0] = Format.INT16;
                    *cast(short*)&store_[Offset] = temp;
                    buffer_.write(store_[0..Offset + short.sizeof]);
                } else {
                    // int 8
                    store_[0] = Format.INT8;
                    store_[1] = take8from!64(value);
                    buffer_.write(store_[0..Offset + byte.sizeof]);
                }
            }
        } else if (value < (1L << 7)) {
            // fixnum
            buffer_.write(take8from!64(value));
        } else {
            if (value < (1L << 16)) {
                if (value < (1L << 8)) {
                    // uint 8
                    store_[0] = Format.UINT8;
                    store_[1] = take8from!64(value);
                    buffer_.write(store_[0..Offset + ubyte.sizeof]);
                } else {
                    // uint 16
                    const temp = convertEndianTo!16(value);

                    store_[0] = Format.UINT16;
                    *cast(ushort*)&store_[Offset] = temp;
                    buffer_.write(store_[0..Offset + ushort.sizeof]);
                }
            } else {
                if (value < (1L << 32)) {
                    // uint 32
                    const temp = convertEndianTo!32(value);

                    store_[0] = Format.UINT32;
                    *cast(uint*)&store_[Offset] = temp;
                    buffer_.write(store_[0..Offset + uint.sizeof]);
                } else {
                    // uint 64
                    const temp = convertEndianTo!64(value);

                    store_[0] = Format.UINT64;
                    *cast(ulong*)&store_[Offset] = temp;
                    buffer_.write(store_[0..Offset + ulong.sizeof]);
                }
            }
        }

        return this;
    }


    /// ditto
    Packer pack(T : float)(in T value)
    {
        union _ { float f; uint i; }

        const temp = convertEndianTo!32(_(value).i);

        store_[0] = Format.FLOAT;
        *cast(uint*)&store_[Offset] = temp;
        buffer_.write(store_[0..1 + float.sizeof]);

        return this;
    }


    /// ditto
    Packer pack(T : double)(in T value)
    {
        union _ { double f; ulong i; }

        const temp = convertEndianTo!64(_(value).i);

        store_[0] = Format.DOUBLE;
        *cast(ulong*)&store_[Offset] = temp;
        buffer_.write(store_);

        return this;
    }


    /// ditto
    Packer pack(T : U[], U)(in T array)// if (isArray!(T))
    {
        if (array is null)
            return packNil();

        // Raw bytes
        static if (isByte!(U) || isSomeChar!(U)) {
            ubyte[] raw = cast(ubyte[])array;

            packRaw(raw.length);
            buffer_.write(raw);
        } else {
            packArray(array.length);

            foreach (elem; array)
                pack(elem);
        }

        return this;
    }


    /// ditto
    Packer pack(T : V[K], V, K)(in T array)// if (isAssociativeArray!(T))
    {
        if (array is null)
            return packNil();

        packMap(array.length);

        foreach (key, value; array) {
            pack(key);
            pack(value);
        }

        return this;
    }


    /**
     * Serializes type-information to buffer.
     *
     * Params:
     *  length = the length of container.
     *
     * Returns:
     *  this.
     */
    Packer packArray(in size_t length)
    {
        if (length < 16) {
            ubyte temp = Format.ARRAY | cast(ubyte)length;
            buffer_.write(take8from(temp));
        } else if (length < 65536) {
            const temp = convertEndianTo!16(length);

            store_[0] = Format.ARRAY16;
            *cast(ushort*)&store_[Offset] = temp;
            buffer_.write(store_[0..Offset + ushort.sizeof]);
        } else {
            const temp = convertEndianTo!32(length);

            store_[0] = Format.ARRAY32;
            *cast(uint*)&store_[Offset] = temp;
            buffer_.write(store_[0..Offset + uint.sizeof]);
        }

        return this;
    }


    /// ditto
    Packer packMap(in size_t length)
    {
        if (length < 16) {
            ubyte temp = Format.MAP | cast(ubyte)length;
            buffer_.write(take8from(temp));
        } else if (length < 65536) {
            const temp = convertEndianTo!16(length);

            store_[0] = Format.MAP16;
            *cast(ushort*)&store_[Offset] = temp;
            buffer_.write(store_[0..Offset + ushort.sizeof]);
        } else {
            const temp = convertEndianTo!32(length);

            store_[0] = Format.MAP32;
            *cast(uint*)&store_[Offset] = temp;
            buffer_.write(store_[0..Offset + uint.sizeof]);
        }

        return this;
    }


    /// ditto
    Packer packRaw(in size_t length)
    {
        if (length < 32) {
            ubyte temp = Format.RAW | cast(ubyte)length;
            buffer_.write(take8from(temp));
        } else if (length < 65536) {
            const temp = convertEndianTo!16(length);

            store_[0] = Format.RAW16;
            *cast(ushort*)&store_[Offset] = temp;
            buffer_.write(store_[0..Offset + ushort.sizeof]);
        } else {
            const temp = convertEndianTo!32(length);

            store_[0] = Format.RAW32;
            *cast(uint*)&store_[Offset] = temp;
            buffer_.write(store_[0..Offset + uint.sizeof]);
        }

        return this;
    }


    /**
     * Serializes the unique value.
     *
     * Returns:
     *  this.
     */
    Packer packNil()
    {
        buffer_.write(Format.NIL);
        return this;
    }


    /// ditto
    Packer packTrue()
    {
        buffer_.write(Format.TRUE);
        return this;
    }


    /// ditto
    Packer packFalse()
    {
        buffer_.write(Format.FALSE);
        return this;
    }
}


/**
 * Helper for $(D Packer) construction.
 *
 * Params:
 *  buffer = the buffer to write.
 */
Packer!(Buffer) packer(Buffer)(Buffer buffer)
{
    return typeof(return)(buffer);
}


private:


template isByte(T)
{
    enum isByte = staticIndexOf!(Unqual!(T), byte, ubyte) >= 0;
}


unittest
{
    static assert(isByte!(byte));
    static assert(isByte!(ubyte));
    static assert(!isByte!(short));
    static assert(!isByte!(ulong));
    static assert(!isByte!(char));
    static assert(!isByte!(string));
}
