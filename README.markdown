[![Build Status](https://travis-ci.org/msgpack/msgpack-d.png)](https://travis-ci.org/msgpack/msgpack-d)

# MessagePack for D

MessagePack is a binary-based JSON-like serialization library.

MessagePack for D is a pure D implementation of MessagePack.

# Features

* Small and High performance
* Zero copy serialization / deserialization
* Stream deserializer / Direct-conversion deserializer
* Support D features(Range, Tuple, real type)

real type is D only. Don't use real-type to communicate other languages.
In addition, Unpacker raises exception if loss of precision occures.

## Limitations

* No subclass through super class reference serialization(API design phase)
* No circular references support

# Install

msgpack-d is only one file. Please copy src/msgpack.d onto your project.

# Usage

msgpack-d is very simple to use:

```D
import std.file;
import msgpack;

struct S { int x; float y; string z; }

void main()
{
    S input = S(10, 25.5, "message");

    // serialize data
    ubyte[] inData = pack(input);

    // write data to a file
    write("file.dat", inData);

    // read data from file
    ubyte[] outData = cast(ubyte[])read("file.dat");

    // unserialize data
    S target = outData.unpack!S();

    // verify data is the same
    assert(target.x == input.x);
    assert(target.y == input.y);
    assert(target.z == input.z);
}
```

See the example directory for more samples.

# Link

* [The MessagePack Project](http://msgpack.org/)

  MessagePack official site

* [MessagePack's issues](https://github.com/msgpack/msgpack-d/issues)

  Github issue

* [MessagePack's Github](http://github.com/msgpack/)

  Other language versions are here

# Copyright

    Copyright (c) 2010- Masahiro Nakagawa

Distributed under the Boost Software License, Version 1.0.
