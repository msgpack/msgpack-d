// Written in the D programming language.

/**
 * MessagePack for D, deserializing routine
 *
 * Copyright: Copyright Masahiro Nakagawa 2010.
 * License:   <a href = "http://www.boost.org/LICENSE_1_0.txt">Boost License 1.0</a>.
 * Authors:   Masahiro Nakagawa
 */
module msgpack.unpacker;

public import msgpack.object;

import std.array;  // for Range

import msgpack.common;

version(unittest) import msgpack.packer, msgpack.buffer;


/**
 * $(D UnpackException) is thrown on parse error
 */
class UnpackException : Exception
{ 
    this(string message)
    { 
        super(message);
    }
}


/**
 * $(D Unpacker) is a $(D MessagePack) stream deserializer
 *
 * $(D Unpacker) becomes a $(D InputRange) if deserialized Array object.
 * This implementation supports zero copy deserialization of Raw object.
 *
 * Example:
-----
...
auto unpacker = unpacker(serializedData);

while(unpacker.execute()) {
    foreach (obj; unpacker) {
        // do stuff
    }
}

if (unpacker.size)
    throw new Exception("Message is too large");
-----
 */
struct Unpacker
{
  private:
    /*
     * Context state of deserialization
     */
    enum State
    {
        HEADER = 0x00,

        // Floating point, Unsigned, Signed interger (== header & 0x03)
        FLOAT = 0x0a,
        DOUBLE,
        UINT8,
        UINT16,
        UINT32,
        UINT64,
        INT8,
        INT16,
        INT32,
        INT64,

        // Container (== header & 0x01)
        RAW16 = 0x1a,
        RAW32,
        ARRAY16,
        ARRAY36,
        MAP16,
        MAP32,
        RAW
    }


    /*
     * Element type of container
     */
    enum ElementType
    {
        ARRAY_ITEM,
        MAP_KEY,
        MAP_VALUE
    }


    /*
     * Internal stack context
     */
    static struct Context
    {
        static struct Container
        {
            ElementType type;    // object container type
            mp_Object   object;  // current object
            mp_Object   key;     // for map object
            size_t      count;   // container length
        }


        State       state;  // current state of deserialization
        size_t      trail;  // current deserializing size
        size_t      top;    // current index of stack
        Container[] stack;  // storing objects
    }


    ubyte[]     buffer_;   // internal buffer
    size_t      limit_;    // size limit of buffer
    size_t      used_;     // index that buffer cosumed
    size_t      offset_;   // index that buffer parsed
    size_t      parsed_;   // total size of parsed message
    bool        hasRaw_;   // indicates whether Raw object has been deserialized
    Context     context_;  // stack environment for streaming deserialization
    mp_Object[] range_;    // for Range operation


  public:
    /**
     * Constructs a $(D Unpacker).
     *
     * Params:
     *  target     = byte buffer to deserialize
     *  bufferSize = size limit of buffer size
     */
    this(in ubyte[] target, size_t bufferSize = 8192)
    in
    {
        assert(target.length);
    }
    body
    {
        const size = target.length;

        expand(bufferSize < size ? size : bufferSize);

        buffer_[0..size] = target;
        used_            = size;

        initializeContext();
    }


    /**
     * Forwards to internal buffer.
     *
     * Returns:
     *  the reference of internal buffer.
     */
    @property nothrow ref ubyte[] buffer()
    {
        return buffer_;
    }


    /**
     * Appends $(D_PARAM target) to internal buffer.
     *
     * Params:
     *  target = new buffer to deserialize.
     */
    void append(in ubyte[] target)
    in
    {
        assert(target.length);
    }
    body
    {
        const size = target.length;

        // lacks current buffer?
        if (limit_ - used_ < size)
            expand(size);

        buffer_[used_..used_ + size] = target;
        used_ += size;
    }


    /**
     * Consumes buffer. This method is helper for buffer property.
     * You must use this method if you write bytes to buffer directly, 
     *
     * Params:
     *  size = the number of consume.
     */
    nothrow void consume(size_t size)
    {
        if (used_ + size > limit_)
            used_ = limit_;
        else
            used_ += size;
    }


    /**
     * Skips unparsed buffer.
     *
     * Params:
     *  size = the number to skip.
     */
    nothrow void skip(in size_t size)
    {
        if (offset_ + size > used_)
            offset_ = used_;
        else
            offset_ += size;
    }


    /**
     * Removes unparsed buffer.
     */
    nothrow void remove()
    {
        used_ = offset_;
    }


    /**
     * Clears some states.
     */
    nothrow void clear()
    {
        initializeContext();

        parsed_ = 0;
    }


    /**
     * Returns:
     *  the total size including unparsed buffer size.
     */
    @property nothrow size_t size() const
    {
        return parsed_ - offset_ + used_;
    }


    /**
     * Returns:
     *  the parsed size.
     */
    @property nothrow size_t parsedSize() const
    {
        return parsed_;
    }


    /**
     * Returns:
     *  the unparsed size of buffer.
     */
    @property nothrow size_t unparsedSize() const
    {
        return used_ - offset_;
    }


    /**
     * Forwards to deserialized object.
     *
     * Returns:
     *  the deserialized object if deserialization completed.
     */
    @property nothrow mp_Object data()
    {
        return context_.stack[0].object;
    }


    /**
     * Executes deserialization.
     *
     * Returns:
     *  true if deserialization has been completed, otherwise false.
     *
     * Throws:
     *  $(D UnpackException) when parse error occurs.
     */
    bool execute()
    {
        /*
         * Current implementation is very durty(goto! goto!! goto!!!).
         * This Complexity for performance(avoid function call).
         */

        bool      ret;
        size_t    cur = offset_;
        mp_Object obj;

        // restores before state
        auto state =  context_.state;
        auto trail =  context_.trail;
        auto top   =  context_.top;
        auto stack = &context_.stack;

        /*
         * Helper for container deserialization
         */
        bool startContainer(string Type)(ElementType type, size_t length)
        {
            mixin("unpack" ~ Type ~ "((*stack)[top].object, length);");

            if (length == 0)
                return false;

            (*stack)[top].type  = type;
            (*stack)[top].count = length;
            (*stack).length     = ++top + 1;

            return true;
        }

        // non-deserialized data is nothing
        if (used_ - offset_ == 0)
            goto Labort;

        do {
          Lstart:
            if (state == State.HEADER) {
                const header = buffer_[cur];

                if (0x00 <= header && header <= 0x7f) {         // positive
                    unpackUInt(obj, header);
                    goto Lpush;
                } else if (0xe0 <= header && header <= 0xff) {  // negative
                    unpackInt(obj, cast(byte)header);
                    goto Lpush;
                } else if (0xa0 <= header && header <= 0xbf) {  // fix raw
                    trail = header & 0x1f;
                    if (trail == 0)
                        goto Lraw;
                    state = State.RAW;
                    cur++;
                    goto Lstart;
                } else if (0x90 <= header && header <= 0x9f) {  // fix array
                    if (!startContainer!"Array"(ElementType.ARRAY_ITEM, header & 0x0f))
                        goto Lpush;
                    goto Lagain;
                } else if (0x80 <= header && header <= 0x8f) {  // fix map
                    if (!startContainer!"Map"(ElementType.MAP_KEY, header & 0x0f))
                        goto Lpush;
                    goto Lagain;
                } else {
                    switch (header) {
                    case Format.UINT8:
                    case Format.UINT16:
                    case Format.UINT32:
                    case Format.UINT64:
                    case Format.INT8:
                    case Format.INT16:
                    case Format.INT32:
                    case Format.INT64:
                    case Format.FLOAT:
                    case Format.DOUBLE:
                        trail = 1 << (header & 0x03); // computes object size
                        state = cast(State)(header & 0x1f);
                        break;
                    case Format.ARRAY16:
                    case Format.ARRAY32:
                    case Format.MAP16:
                    case Format.MAP32:
                    case Format.RAW16:
                    case Format.RAW32:
                        trail = 2 << (header & 0x01);  // computes container size
                        state = cast(State)(header & 0x1f);
                        break;
                    case Format.NIL:
                        unpackNil(obj);
                        goto Lpush;
                    case Format.TRUE:
                        unpackBool(obj, true);
                        goto Lpush;
                    case Format.FALSE:
                        unpackBool(obj, false);
                        goto Lpush;
                    default:
                        throw new UnpackException("Unknown format");
                    }

                    cur++;
                    goto Lstart;
                }
            } else {
                // data lack for deserialization
                if (used_ - cur < trail)
                    goto Labort;

                const base = cur; cur += trail - 1;  // fix current position

                final switch (state) {
                case State.FLOAT:
                    union _f { uint i; float f; };
                    _f temp;

                    temp.i = load32To!uint(buffer_[base..base + trail]);
                    unpackDouble(obj, temp.f);
                    goto Lpush;
                case State.DOUBLE:
                    union _d { ulong i; double f; };
                    _d temp;

                    temp.i = load64To!long(buffer_[base..base + trail]);
                    unpackDouble(obj, temp.f);
                    goto Lpush;
                case State.UINT8:
                    unpackUInt(obj, buffer_[base]);
                    goto Lpush;
                case State.UINT16:
                    unpackUInt(obj, load16To!ulong(buffer_[base..base + trail]));
                    goto Lpush;
                case State.UINT32:
                    unpackUInt(obj, load32To!ulong(buffer_[base..base + trail]));
                    goto Lpush;
                case State.UINT64:
                    unpackUInt(obj, load64To!ulong(buffer_[base..base + trail]));
                    goto Lpush;
                case State.INT8:
                    unpackInt(obj, cast(byte)buffer_[base]);
                    goto Lpush;
                case State.INT16:
                    unpackInt(obj, load16To!long(buffer_[base..base + trail]));
                    goto Lpush;
                case State.INT32:
                    unpackInt(obj, load32To!long(buffer_[base..base + trail]));
                    goto Lpush;
                case State.INT64:
                    unpackInt(obj, load64To!long(buffer_[base..base + trail]));
                    goto Lpush;
                case State.RAW: Lraw:
                    hasRaw_ = true;
                    unpackRaw(obj, buffer_[base..base + trail]);
                    goto Lpush;
                case State.RAW16:
                    trail = load16To!uint(buffer_[base..base + trail]);
                    if (trail)
                        goto Lraw;
                    state = State.RAW;
                    cur++;
                    goto Lstart;
                case State.RAW32:
                    trail = load32To!uint(buffer_[base..base + trail]);
                    if (trail)
                        goto Lraw;
                    state = State.RAW;
                    cur++;
                    goto Lstart;
                case State.ARRAY16:
                    if (!startContainer!"Array"(ElementType.ARRAY_ITEM,
                                                load16To!size_t(buffer_[base..base + trail])))
                        goto Lpush;
                    goto Lagain;
                case State.ARRAY36:
                    if (!startContainer!"Array"(ElementType.ARRAY_ITEM,
                                                load32To!size_t(buffer_[base..base + trail])))
                        goto Lpush;
                    goto Lagain;
                case State.MAP16:
                    if (!startContainer!"Map"(ElementType.MAP_KEY,
                                              load16To!size_t(buffer_[base..base + trail])))
                        goto Lpush;
                    goto Lagain;
                case State.MAP32:
                    if (!startContainer!"Map"(ElementType.MAP_KEY,
                                              load32To!size_t(buffer_[base..base + trail])))
                        goto Lpush;
                    goto Lagain;
                case State.HEADER:
                    break;
                }      
            }

          Lpush:
            if (top == 0)
                goto Lfinish;

            auto container = &(*stack)[top - 1];

            final switch (container.type) {
            case ElementType.ARRAY_ITEM:
                container.object.via.array ~= obj;
                if (--container.count == 0) {
                    obj = container.object;
                    top--;
                    goto Lpush;
                }
                break;
            case ElementType.MAP_KEY:
                container.key  = obj;
                container.type = ElementType.MAP_VALUE;
                break;
            case ElementType.MAP_VALUE:
                container.object.via.map ~= mp_KeyValue(container.key, obj);
                if (--container.count == 0) {
                    obj = container.object;
                    top--;
                    goto Lpush;
                }
                container.type = ElementType.MAP_KEY;
            }

          Lagain:
            state = State.HEADER;
            cur++;
        } while (cur < used_);

        goto Labort;

      Lfinish:
        if (obj.type == mp_Type.ARRAY)
            range_= obj.via.array;
        (*stack)[0].object = obj;
        ret = true;
        cur++;
        goto Lend;

      Labort:
        ret = false;

      Lend:
        context_.state = state;
        context_.trail = trail;
        context_.top   = top;
        parsed_       += cur - offset_;
        offset_        = cur;

        return ret;
    }


    /// InputRange implementations.

    /**
     * Range primitive operation that checks iteration state.
     *
     * Returns:
     *  true if there are no more elements to be iterated.
     */
    @property bool empty() const  // std.array.empty isn't nothrow function
    {
        return range_.empty;
    }


    /**
     * Range primitive operation that returns the currently iterated element.
     *
     * Returns:
     *  the deserialized $(D mp_Object).
     */
    @property mp_Object front()
    {
        return range_.front();
    }


    /**
     * Range primitive operation that advances the range to its next element.
     */
    void popFront()
    {
        range_.popFront();
    }


  private:
    /**
     * initializes internal stack environment.
     */
    nothrow void initializeContext()
    {
        context_.state        = State.HEADER;
        context_.trail        = 0;
        context_.top          = 0;
        context_.stack.length = 1;
    }


    /**
     * Expands internal buffer.
     *
     * Params:
     *  size = new buffer size.
     */
    void expand(size_t size)
    {
        // rewinds buffer(completed deserialization)
        if (used_ == offset_ && !hasRaw_) {
            used_ =  offset_ = 0;

            if (limit_ >= size)
                return;
        }

        if (limit_ < used_ + size)
            limit_ = used_ + size;

        // deserializing state is mid-flow(buffer has non-parsed data yet)
        if (offset_) {
            auto notParsed  = used_ - offset_;
            auto restBuffer = buffer_[offset_..used_];

            if (hasRaw_) {
                hasRaw_ = false;
                buffer_ = new ubyte[](limit_);
            } else {
                restBuffer = restBuffer.dup;  // avoids overlapping copy
            }

            buffer_[0..notParsed] = restBuffer;
            used_   = notParsed;
            offset_ = 0;
        } else {
            buffer_.length = limit_;
        }
    }
}


/**
 * Helper for $(D Unpacker) construction.
 *
 * Params:
 *  target     = byte buffer to deserialize.
 *  bufferSize = size limit of buffer size.
 *
 * Returns:
 *  a $(D Unpacker) object instantiated and initialized according to the arguments.
 */
Unpacker unpacker(in ubyte[] target, size_t bufferSize = 8192)
{
    return typeof(return)(target, bufferSize);
}


unittest
{
    SimpleBuffer buffer;
    auto packer = packer(&buffer);
    enum Size   = mp_Type.max;

    packer.packArray(Size);
    packer.packNil().packTrue().pack(1).pack(-2).pack("Hi!").pack([1]).pack([1:1]);

    auto unpacker = unpacker(packer.buffer.data);

    unpacker.execute();

    // Range test
    foreach (unused; 0..2) {
        uint i;

        foreach (obj; unpacker) i++;
        assert(i == Size);
    }

    auto result = unpacker.data.via.array;

    assert(result[0].type          == mp_Type.NIL);
    assert(result[1].via.boolean   == true);
    assert(result[2].via.uinteger  == 1);
    assert(result[3].via.integer   == -2);
    assert(result[4].via.raw       == [72, 105, 33]);
    assert(result[5].as!(int[])    == [1]);
    assert(result[6].as!(int[int]) == [1:1]);

    unpacker.clear();
}


private:


/**
 * Sets object type and value.
 *
 * Params:
 *  object = the object to set
 *  value  = the content to set
 */
void unpackUInt(ref mp_Object object, ulong value)
{
    object.type         = mp_Type.POSITIVE_INTEGER;
    object.via.uinteger = value;
}


/// ditto
void unpackInt(ref mp_Object object, long value)
{
    object.type        = mp_Type.NEGATIVE_INTEGER;
    object.via.integer = value;
}


/// ditto
void unpackDouble(ref mp_Object object, double value)
{
    object.type         = mp_Type.FLOAT;
    object.via.floating = value;
}


/// ditto
void unpackRaw(ref mp_Object object, ubyte[] raw)
{
    object.type    = mp_Type.RAW;
    object.via.raw = raw;
}


/// ditto
void unpackArray(ref mp_Object object, size_t length)
{
    object.type = mp_Type.ARRAY;
    object.via.array.length = 0;
    object.via.array.reserve(length);
}


/// ditto
void unpackMap(ref mp_Object object, size_t length)
{
    object.type = mp_Type.MAP;
    object.via.map.length = 0;
    object.via.map.reserve(length);
}


/// ditto
void unpackNil(ref mp_Object object)
{
    object.type = mp_Type.NIL;
}


/// ditto
void unpackBool(ref mp_Object object, bool value)
{
    object.type        = mp_Type.BOOLEAN;
    object.via.boolean = value;
}


unittest
{
    mp_Object object;

    // Unsigned integer
    unpackUInt(object, uint.max);
    assert(object.type         == mp_Type.POSITIVE_INTEGER);
    assert(object.via.uinteger == uint.max);

    // Signed integer
    unpackInt(object, int.min);
    assert(object.type        == mp_Type.NEGATIVE_INTEGER);
    assert(object.via.integer == int.min);

    // Floating point
    unpackDouble(object, double.max);
    assert(object.type         == mp_Type.FLOAT);
    assert(object.via.floating == double.max);

    // Raw
    unpackRaw(object, cast(ubyte[])[1]);
    assert(object.type    == mp_Type.RAW);
    assert(object.via.raw == cast(ubyte[])[1]);

    // Array
    mp_Object[] array; array.reserve(16);

    unpackArray(object, 16);
    assert(object.type               == mp_Type.ARRAY);
    assert(object.via.array.capacity == array.capacity);

    // Map
    mp_KeyValue[] map; map.reserve(16);

    unpackMap(object, 16);
    assert(object.type             == mp_Type.MAP);
    assert(object.via.map.capacity == map.capacity);

    // NIL
    unpackNil(object);
    assert(object.type == mp_Type.NIL);

    // Bool
    unpackBool(object, true);
    assert(object.type        == mp_Type.BOOLEAN);
    assert(object.via.boolean == true);
}
