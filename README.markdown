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

See example directory.

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
