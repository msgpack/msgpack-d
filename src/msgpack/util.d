// Written in the D programming language.

/**
 * MessagePack for D, some utilities
 *
 * Copyright: Copyright Masahiro Nakagawa 2010.
 * License:   <a href = "http://www.boost.org/LICENSE_1_0.txt">Boost License 1.0</a>.
 * Authors:   Masahiro Nakagawa
 */
module msgpack.util;

public
{
    import msgpack.object;
    import msgpack.unpacker;
}

version(unittest) import msgpack.packer, msgpack.buffer;


@trusted:


/**
 * Handy helper for creating MessagePackable object.
 *
 * mp_pack/mp_unpack are special methods for serialization/deserialization.
 * This template provides those methods to struct/class.
 *
 * Example:
-----
struct S
{
    int num; string str;
    mixin MessagePackable;  // all members
    // mixin MessagePackable!("num");  // num only
}
-----
 */
mixin template MessagePackable(Members...)
{
    static if (Members.length == 0) {
        /**
         * Serializes members using $(D_PARAM packer).
         *
         * Params:
         *  packer = the serializer to pack.
         */
        void mp_pack(Packer)(ref Packer packer) const
        {
            packer.packArray(this.tupleof.length);
            foreach (member; this.tupleof)
                packer.pack(member);
        }


        /**
         * Deserializes $(D MessagePack) object to members for stream deserializer.
         *
         * Params:
         *  object = the MessagePack object to unpack.
         *
         * Throws:
         *  InvalidTypeException if $(D_PARAM object) is not Array type.
         */
        void mp_unpack(mp_Object object)
        {
            if (object.type != mp_Type.ARRAY)
                throw new InvalidTypeException("mp_Object must be Array type");

            foreach (i, member; this.tupleof)
                this.tupleof[i] = object.via.array[i].as!(typeof(member));
        }


        /**
         * Deserializes $(D MessagePack) object to members for direct conversion deserializer.
         *
         * Params:
         *  object = the reference to direct conversion deserializer.
         *
         * Throws:
         *  InvalidTypeException if deserialized object size is mismatched.
         */
        void mp_unpack(ref Unpacker!(false) unpacker)
        {
            auto length = unpacker.unpackArray();
            if (length != this.tupleof.length)
                throw new InvalidTypeException("Deserialized object size is mismatched");

            foreach (i, member; this.tupleof)
                unpacker.unpack(this.tupleof[i]);
        }
    } else {
        /**
         * Member selecting version of mp_pack.
         */
        void mp_pack(Packer)(ref Packer packer) const
        {
            packer.packArray(Members.length);
            foreach (member; Members)
                packer.pack(mixin(member));
        }


        /**
         * Member selecting version of mp_unpack for stream deserializer.
         */
        void mp_unpack(mp_Object object)
        {
            if (object.type != mp_Type.ARRAY)
                throw new InvalidTypeException("mp_Object must be Array type");

            foreach (i, member; Members)
                mixin(member ~ "= object.via.array[i].as!(typeof(" ~ member ~ "));");
        }


        /**
         * Member selecting version of mp_unpack for direct converion deserializer.
         */
        void mp_unpack(ref Unpacker!(false) unpacker)
        {
            auto length = unpacker.unpackArray();
            if (length != Members.length)
                throw new InvalidTypeException("Deserialized object size is mismatched");

            foreach (member; Members)
                unpacker.unpack(mixin(member));
        }
    }
}


unittest
{
    { // all members
        static struct S
        {
            uint num; string str;
            mixin MessagePackable;
        }

        mixin DefinePacker;

        S orig = S(10, "Hi!"); orig.mp_pack(packer);

        { // stream
            auto unpacker = unpacker(packer.buffer.data); unpacker.execute();

            S result; result.mp_unpack(unpacker.unpacked);

            assert(result.num == 10);
            assert(result.str == "Hi!");
        }
        { // direct conversion
            auto unpacker = unpacker!(false)(packer.buffer.data);

            S result; unpacker.unpack(result);

            assert(result.num == 10);
            assert(result.str == "Hi!");
        }
    }
    { // member select
        static class C
        {
            uint num; string str;

            this() {}
            this(uint n, string s) { num = n; str = s; }

            mixin MessagePackable!("num");
        }

        mixin DefinePacker;

        C orig = new C(10, "Hi!"); orig.mp_pack(packer);

        { // stream
            auto unpacker = unpacker(packer.buffer.data); unpacker.execute();

            C result = new C; result.mp_unpack(unpacker.unpacked);

            assert(result.num == 10);
        }
        { // direct conversion
            auto unpacker = unpacker!(false)(packer.buffer.data);

            C result; unpacker.unpack(result);

            assert(result.num == 10);
        }
    }
}
