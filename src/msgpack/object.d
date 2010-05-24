// Written in the D programming language.

/**
 * MessagePack for D, static resolution routine
 *
 * Copyright: Copyright Masahiro Nakagawa 2010.
 * License:   <a href = "http://www.boost.org/LICENSE_1_0.txt">Boost License 1.0</a>.
 * Authors:   Masahiro Nakagawa
 */
module msgpack.object;

/*
 * Avoids compile error related object module. Bug or Spec?
 * http://d.puremagic.com/issues/show_bug.cgi?id = 4102
 */
import object;

import std.traits;

version(unittest) import std.typecons;


@trusted:


/**
 * $(D MessagePack) object type
 */
enum mp_Type
{
    NIL,
    BOOLEAN,
    POSITIVE_INTEGER,
    NEGATIVE_INTEGER,
    FLOAT,  // Original version is DOUBLE
    ARRAY,
    MAP,
    RAW
}


/**
 * $(D InvalidTypeException) is thrown on type errors
 */
class InvalidTypeException : Exception
{
    this(string message)
    {
        super(message);
    }
}


/**
 * $(D mp_Object) is a $(D MessagePack) Object representation
 */
struct mp_Object
{
    static union Value
    {
        bool          boolean;
        ulong         uinteger;
        long          integer;
        real          floating;
        mp_Object[]   array;
        mp_KeyValue[] map;
        ubyte[]       raw;
    }


    mp_Type type;  /// represents object type 
    Value   via;   /// represents real value


    /**
     * Constructs a $(D mp_Object) with arguments.
     *
     * Params:
     *  value   = the real content.
     *  mp_type = the type of object.
     */
    this(mp_Type mp_type = mp_Type.NIL)
    {
        type = mp_type;
    }


    /// ditto
    this(bool value, mp_Type mp_type = mp_Type.BOOLEAN)
    {
        this(mp_type);
        via.boolean = value;
    }


    /// ditto
    this(ulong value, mp_Type mp_type = mp_Type.POSITIVE_INTEGER)
    {
        this(mp_type);
        via.uinteger = value;
    }


    /// ditto
    this(long value, mp_Type mp_type = mp_Type.NEGATIVE_INTEGER)
    {
        this(mp_type);
        via.integer = value;
    }


    /// ditto
    this(real value, mp_Type mp_type = mp_Type.FLOAT)
    {
        this(mp_type);
        via.floating = value;
    }


    /// ditto
    this(mp_Object[] value, mp_Type mp_type = mp_Type.ARRAY)
    {
        this(mp_type);
        via.array = value;
    }


    /// ditto
    this(mp_KeyValue[] value, mp_Type mp_type = mp_Type.MAP)
    {
        this(mp_type);
        via.map = value;
    }


    /// ditto
    this(ubyte[] value, mp_Type mp_type = mp_Type.RAW)
    {
        this(mp_type);
        via.raw = value;
    }


    /**
     * Converts to $(D_PARAM T) type.
     *
     * Returns:
     *  converted value.
     *
     * Throws:
     *  InvalidTypeException if type is mismatched.
     *
     * NOTE:
     *  Current implementation uses cast.
     */
    @property T as(T)() if (is(T == bool))
    {
        if (type != mp_Type.BOOLEAN)
            onCastError();

        return cast(bool)via.boolean;
    }


    /// ditto
    @property T as(T)() if (isIntegral!T)
    {
        if (type == mp_Type.POSITIVE_INTEGER)
            return cast(T)via.uinteger;

        if (type == mp_Type.NEGATIVE_INTEGER)
            return cast(T)via.integer;

        onCastError();

        assert(false);
    }


    /// ditto
    @property T as(T)() if (isFloatingPoint!T)
    {
        if (type != mp_Type.FLOAT)
            onCastError();

        return cast(T)via.floating;
    }


    /// ditto
    @property T as(T)() if (isArray!T)
    {
        if (type == mp_Type.NIL)
            return null;

        static if (isSomeString!T) {
            if (type != mp_Type.RAW)
                onCastError();

            return cast(T)via.raw;
        } else {
            alias typeof(T.init[0]) V;

            if (type != mp_Type.ARRAY)
                onCastError();

            V[] array;

            foreach (elem; via.array)
                array ~= elem.as!(V);

            return array;
        }
    }


    /// ditto
    @property T as(T)() if (isAssociativeArray!T)
    {
        alias typeof(T.init.keys[0])   K;
        alias typeof(T.init.values[0]) V;

        if (type == mp_Type.NIL)
            return null;

        if (type != mp_Type.MAP)
            onCastError();

        V[K] map;

        foreach (elem; via.map)
            map[elem.key.as!(K)] = elem.value.as!(V);

        return map;
    }


    /**
     * Converts to $(D_PARAM T) type.
     *
     * $(D_KEYWORD struct) and $(D_KEYWORD class) need to implement $(D mp_unpack) method.
     * $(D mp_unpack) signature is:
     * -----
     * void mp_unpack(mp_Object object)
     * -----
     * Assumes $(D std.typecons.Tuple) if $(D_KEYWORD struct) doens't implement $(D mp_unpack).
     *
     * Params:
     *  args = arguments to class constructor(class only).
     *
     * Returns:
     *  converted value.
     */
    @property T as(T, Args...)(Args args) if (is(T == class))
    {
        static if (!__traits(compiles, { T t; t.mp_unpack(this); }))
            static assert(false, T.stringof ~ " is not a MessagePackable object");

        if (type == mp_Type.NIL)
            return null;

        T object = new T(args);

        object.mp_unpack(this);

        return object;
    }


    /// ditto
    @property T as(T)() if (is(T == struct))
    {
        T obj;

        static if (__traits(compiles, { T t; t.mp_unpack(this); })) {
            obj.mp_unpack(this);
        } else {
            foreach (i, Type; T.Types)
                obj.field[i] = via.array[i].as!(Type);
        }

        return obj;
    }


    /**
     * Special method called by $(D Packer).
     *
     * Params:
     *  packer = a serializer.
     */
    void mp_pack(Packer)(ref Packer packer) const
    {
        final switch (type) {
        case mp_Type.NIL:
            packer.packNil();
            break;
        case mp_Type.BOOLEAN:
            packer.pack(via.boolean);
            break;
        case mp_Type.POSITIVE_INTEGER:
            packer.pack(via.uinteger);
            break;
        case mp_Type.NEGATIVE_INTEGER:
            packer.pack(via.integer);
            break;
        case mp_Type.FLOAT:
            packer.pack(via.floating);
            break;
        case mp_Type.RAW:
            packer.pack(via.raw);
            break;
        case mp_Type.ARRAY:
            packer.packArray(via.array.length);
            foreach (elem; via.array)
                elem.mp_pack(packer);
            break;
        case mp_Type.MAP:
            packer.packMap(via.map.length);
            foreach (kv; via.map) {
                kv.key.mp_pack(packer);
                kv.value.mp_pack(packer);
            }
            break;
        }
    }


    /**
     * Comparison for equality.
     */
    bool opEquals(Tdummy = void)(ref const mp_Object other) const
    {
        if (type != other.type)
            return false;

        final switch (type) {
        case mp_Type.NIL:              return true;
        case mp_Type.BOOLEAN:          return via.boolean  == other.via.boolean;
        case mp_Type.POSITIVE_INTEGER: return via.uinteger == other.via.uinteger;
        case mp_Type.NEGATIVE_INTEGER: return via.integer  == other.via.integer;
        case mp_Type.FLOAT:            return via.floating == other.via.floating;
        case mp_Type.RAW:              return via.raw      == other.via.raw;
        case mp_Type.ARRAY:            return via.array    == other.via.array;
        case mp_Type.MAP:              return via.map      == other.via.map;
        }
    }


    /// ditto
    bool opEquals(T : bool)(in T other) const
    {
        if (type != mp_Type.BOOLEAN)
            return false;

        return via.boolean == other;
    }


    /// ditto
    bool opEquals(T : ulong)(in T other) const
    {
        static if (__traits(isUnsigned, T)) {
            if (type != mp_Type.POSITIVE_INTEGER)
                return false;

            return via.uinteger == other;
        } else {
            if (type != mp_Type.NEGATIVE_INTEGER)
                return false;

            return via.integer == other;
        }
    }


    /// ditto
    bool opEquals(T : real)(in T other) const
    {
        if (type != mp_Type.FLOAT)
            return false;

        return via.floating == other;
    }


    /// ditto
    bool opEquals(T : mp_Object[])(in T other) const
    {
        if (type != mp_Type.ARRAY)
            return false;

        return via.array == other;
    }


    /// ditto
    bool opEquals(T : mp_KeyValue[])(in T other) const
    {
        if (type != mp_Type.MAP)
            return false;

        return via.map == other;
    }


    /// ditto
    bool opEquals(T : ubyte[])(in T other) const
    {
        if (type != mp_Type.RAW)
            return false;

        return via.raw == other;
    }
}


/**
 * $(D mp_KeyValue) is a $(D MessagePack) Map Object representation
 */
struct mp_KeyValue
{
    mp_Object key;    /// represents key of Map
    mp_Object value;  /// represents value of Map


    /**
     * Comparison for equality.
     */
    bool opEquals(ref const mp_KeyValue other) const
    {
        return key == other.key && value == other.value;
    }
}


private:


void onCastError()
{
    throw new InvalidTypeException("Attempt to cast with another type");
}


unittest
{
    // nil
    mp_Object object = mp_Object();
    mp_Object other  = mp_Object();

    assert(object      == other);
    assert(object.type == mp_Type.NIL);

    // boolean
    object = mp_Object(true);
    other  = mp_Object(false);

    assert(object           != other);
    assert(object.type      == mp_Type.BOOLEAN);
    assert(object.as!(bool) == true);
    assert(other            == false);

    try {
        auto b = object.as!(uint);
        assert(false);
    } catch (InvalidTypeException e) { }

    // unsigned integer
    object = mp_Object(10UL);
    other  = mp_Object(10UL);

    assert(object           == other);
    assert(object.type      == mp_Type.POSITIVE_INTEGER);
    assert(object.as!(uint) == 10);
    assert(other            == 10UL);

    // signed integer
    object = mp_Object(-20L);
    other  = mp_Object(-10L);

    assert(object          != other);
    assert(object.type     == mp_Type.NEGATIVE_INTEGER);
    assert(object.as!(int) == -20);
    assert(other           == -10L);

    // floating point
    object = mp_Object(0.1e-10L);
    other  = mp_Object(0.1e-20L);

    assert(object           != other);
    assert(object.type      == mp_Type.FLOAT);
    assert(object.as!(real) == 0.1e-10L);
    assert(other            == 0.1e-20L);

    // raw
    object = mp_Object(cast(ubyte[])[72, 105, 33]);
    other  = mp_Object(cast(ubyte[])[72, 105, 33]);

    assert(object             == other);
    assert(object.type        == mp_Type.RAW);
    assert(object.as!(string) == "Hi!");
    assert(other              == cast(ubyte[])[72, 105, 33]);

    // array
    auto t = mp_Object(cast(ubyte[])[72, 105, 33]);
    object = mp_Object([t]);
    other  = mp_Object([t]);

    assert(object               == other);
    assert(object.type          == mp_Type.ARRAY);
    assert(object.as!(string[]) == ["Hi!"]);
    assert(other                == [t]);

    // map
    object = mp_Object([mp_KeyValue(mp_Object(1L), mp_Object(2L))]);
    other  = mp_Object([mp_KeyValue(mp_Object(1L), mp_Object(1L))]);

    assert(object               != other);
    assert(object.type          == mp_Type.MAP);
    assert(object.as!(int[int]) == [1:2]);
    assert(other                == [mp_KeyValue(mp_Object(1L), mp_Object(1L))]);

    object = mp_Object(10UL);

    // struct
    static struct S
    {
        ulong num;

        void mp_unpack(mp_Object object) { num = object.via.uinteger; }
    }

    S s = object.as!(S);
    assert(s.num == 10);

    // class
    static class C
    {
        ulong num;

        void mp_unpack(mp_Object object) { num = object.via.uinteger; }
    }

    C c = object.as!(C);
    assert(c.num == 10);

    // std.typecons.Tuple
    object = mp_Object([mp_Object(true), mp_Object(1UL), mp_Object(cast(ubyte[])"Hi!")]);

    auto tuple = object.as!(Tuple!(bool, uint, string));
    assert(tuple.field[0] == true);
    assert(tuple.field[1] == 1u);
    assert(tuple.field[2] == "Hi!");

    /* 
     * non-MessagePackable object is stopped by static assert
     * static struct NonMessagePackable {}
     * auto nonMessagePackable = object.as!(NonMessagePackable);
     */
}
