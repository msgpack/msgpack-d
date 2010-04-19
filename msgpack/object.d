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
 */
import object;

import std.traits;


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
        double        floating;
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
    this(double value, mp_Type mp_type = mp_Type.FLOAT)
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
            raise();

        return cast(bool)via.boolean;
    }


    /// ditto
    @property T as(T)() if (isIntegral!(T))
    {
        if (type == mp_Type.POSITIVE_INTEGER)
            return cast(T)via.uinteger;

        if (type == mp_Type.NEGATIVE_INTEGER)
            return cast(T)via.integer;

        raise();

        assert(false);
    }


    /// ditto
    @property T as(T)() if (isFloatingPoint!(T))
    {
        if (type != mp_Type.FLOAT)
            raise();

        return cast(T)via.floating;
    }


    /// ditto
    @property T as(T)() if (isArray!(T))
    {
        if (type == mp_Type.NIL)
            return null;

        static if (isSomeString!(T)) {
            if (type != mp_Type.RAW)
                raise();

            return cast(T)via.raw;
        } else {
            alias typeof(T.init[0]) V;

            if (type != mp_Type.ARRAY)
                raise();

            V[] array;

            foreach (elem; via.array)
                array ~= elem.as!(V);

            return array;
        }
    }


    /// ditto
    @property T as(T)() if (isAssociativeArray!(T))
    {
        alias typeof(T.init.keys[0])   K;
        alias typeof(T.init.values[0]) V;

        if (type == mp_Type.NIL)
            return null;

        if (type != mp_Type.MAP)
            raise();

        V[K] map;

        foreach (elem; via.map)
            map[elem.key.as!(K)] = elem.value.as!(V);

        return map;
    }


    /**
     * Comparison for equality.
     */
    bool opEquals(ref const mp_Object other) const
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


  private:
    void raise()
    {
        throw new InvalidTypeException("Attempt to cast with another type");
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

    // unsigned integer
    object = mp_Object(10UL);
    other  = mp_Object(10UL);

    assert(object           == other);
    assert(object.type      == mp_Type.POSITIVE_INTEGER);
    assert(object.as!(uint) == 10);

    // signed integer
    object = mp_Object(-20L);
    other  = mp_Object(-10L);

    assert(object          != other);
    assert(object.type     == mp_Type.NEGATIVE_INTEGER);
    assert(object.as!(int) == -20);

    // floating point
    object = mp_Object(0.1e-10);
    other  = mp_Object(0.1e-20);

    assert(object             != other);
    assert(object.type        == mp_Type.FLOAT);
    assert(object.as!(double) == 0.1e-10);

    // raw
    object = mp_Object(cast(ubyte[])[72, 105, 33]);
    other  = mp_Object(cast(ubyte[])[72, 105, 33]);

    assert(object             == other);
    assert(object.type        == mp_Type.RAW);
    assert(object.as!(string) == "Hi!");

    // array
    object = mp_Object([object]);
    other  = mp_Object([other]);

    assert(object               == other);
    assert(object.type          == mp_Type.ARRAY);
    assert(object.as!(string[]) == ["Hi!"]);

    // map
    object = mp_Object([mp_KeyValue(mp_Object(1L), mp_Object(2L))]);
    other  = mp_Object([mp_KeyValue(mp_Object(1L), mp_Object(1L))]);

    assert(object               != other);
    assert(object.type          == mp_Type.MAP);
    assert(object.as!(int[int]) == [1:2]);
}
