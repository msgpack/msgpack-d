DMD     = dmd
LIB     = libmsgpack.a
DFLAGS  = -O -release -inline -nofloat -w -d -Isrc
UDFLAGS = -w -g -debug -unittest
NAMES   = buffer common msgpack object packer unpacker util
MODULES = $(addprefix msgpack/, $(NAMES))
FILES   = $(addsuffix .d, $(MODULES))
SRCS    = $(addprefix src/, $(FILES))

# DDoc
DOCS      = $(addsuffix .html, $(NAMES))
DOCDIR    = html
CANDYDOC  = $(addprefix html/candydoc/, candy.ddoc modules.ddoc)
DDOCFLAGS = -Dd$(DOCDIR) -c -o- -Isrc $(CANDYDOC)

target: doc $(LIB)

$(LIB):
	$(DMD) $(DFLAGS) -lib -of$(LIB) $(SRCS)

doc:
	$(DMD) $(DDOCFLAGS) $(SRCS)

clean:
	rm $(addprefix $(DOCDIR)/, $(DOCS)) $(LIB)
