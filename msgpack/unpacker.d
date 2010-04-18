// Written in the D programming language.

/**
 * MessagePack for D, deserializing routine
 *
 * Copyright: Copyright Masahiro Nakagawa 2010.
 * License:   <a href = "http://www.boost.org/LICENSE_1_0.txt">Boost License 1.0</a>.
 * Authors:   Masahiro Nakagawa
 */
module msgpack.unpacker;

import msgpack.common;
import msgpack.object;


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
 * This implementation supports zero copy deserialization of Raw object.
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

        // Floating point, Unsigned, Signed interger
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

        // Container
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


    ubyte[] buffer_;   // internal buffer
    size_t  limit_;    // size limit of buffer
    size_t  used_;     // index that buffer cosumed
    size_t  offset_;   // index that buffer parsed
    size_t  parsed_;   // total size of parsed message
    bool    hasRaw_;   // indicates whether Raw object has been deserialized
    Context context_;  // stack environment for streaming deserialization


  public:
    /**
     * Constructs a $(D Unpacker).
     *
     * Params:
     *  target     = byte buffer to deserialize
     *  bufferSize = size limit of buffer size
     */
    this(ubyte[] target, size_t bufferSize = 8192)
    in
    {
        assert(target.length);
    }
    body
    {
        const size = target.length;

        expandBuffer(bufferSize < size ? size : bufferSize);

        buffer_[0..size] = target;
        used_            = size;

        initializeContext();
    }


    /**
     * Appends $(D target) to internal buffer.
     *
     * Params:
     *  target = new buffer to deserialize.
     */
    void append(ubyte[] target)
    {
        const size = target.length;

        // lacks current buffer?
        if (limit_ - used_ < size)
            expandBuffer(size);

        buffer_[used_..used_ + size] = target;
        used_ += size;
    }


    /**
     * Range primitive operation that executes deserialization.
     *
     * Returns:
     *  true if deserialization completed, otherwize false.
     *
     * Throws:
     *  $(D UnpackException) when parse error occurs.
     */
    @property bool empty()
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
        (*stack)[0].object = obj; 
        ret = false;
        cur++;
        goto Lend;

      Labort:
        ret = true;

      Lend:
        context_.state = state;
        context_.trail = trail;
        context_.top   = top;
        parsed_       += cur - offset_;
        offset_        = cur;

        return ret;
    }


    /**
     * Range primitive operation that returns the currently element.
     *
     * Returns:
     *  the deserialized $(D mp_Object).
     */
    @property mp_Object front()
    {
        return context_.stack[0].object;
    }


    /**
     * Range primitive operation that resets environment.
     */
    void popFront()
    {
        initializeContext();
        parsed_  = 0;
    }


  private:
    /**
     * initializes internal stack environment.
     */
    void initializeContext()
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
    void expandBuffer(size_t size)
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
 *  target     = byte buffer to deserialize
 *  bufferSize = size limit of buffer size
 */
Unpacker unpacker(ubyte[] target, size_t bufferSize = 8192)
{
    return typeof(return)(target, bufferSize);
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
    object.via.array.reserve(length);
}


/// ditto
void unpackMap(ref mp_Object object, size_t length)
{
    object.type = mp_Type.MAP;
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
