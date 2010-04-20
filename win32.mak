DMD     = dmd
LIB     = msgpack.lib
DFLAGS  = -O -release -inline -nofloat -w -d -Isrc
UDFLAGS = -w -g -debug -unittest
NAMES   = buffer common msgpack object packer unpacker util
MODULES = $(addprefix msgpack\, $(NAMES))
FILES   = $(addsuffix .d, $(MODULES))

SRCS = \
	src\msgpack\buffer.d \
	src\msgpack\common.d \
	src\msgpack\msgpack.d \
	src\msgpack\object.d \
	src\msgpack\packer.d \
	src\msgpack\unpacker.d \
	src\msgpack\util.d

# DDoc
DOCDIR    = html
CANDYDOC  = html\candydoc\candy.ddoc html\candydoc\modules.ddoc
DDOCFLAGS = -Dd$(DOCDIR) -c -o- -Isrc $(CANDYDOC)

DOCS = \
	$(DOCDIR)\buffer.html \
	$(DOCDIR)\common.html \
	$(DOCDIR)\msgpack.html \
	$(DOCDIR)\object.html \
	$(DOCDIR)\packer.html \
	$(DOCDIR)\unpacker.html \
	$(DOCDIR)\util.html

target: doc $(LIB)

$(LIB):
	$(DMD) $(DFLAGS) -lib -of$(LIB) $(SRCS)

doc:
	$(DMD) $(DDOCFLAGS) $(SRCS)

clean:
	rm $(DOCS) $(LIB)
