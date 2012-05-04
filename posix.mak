# build mode: 32bit or 64bit
ifeq (,$(MODEL))
	MODEL := 64
endif

ifeq (,$(DMD))
	DMD := dmd
endif

LIB     = libmsgpack.a
DFLAGS  = -Isrc -m$(MODEL) -w -d -property

ifeq ($(BUILD),debug)
	DFLAGS += -g -debug
else
	DFLAGS += -O -release -nofloat -inline
endif

NAMES = msgpack
FILES = $(addsuffix .d, $(NAMES))
SRCS  = $(addprefix src/, $(FILES))

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

MAIN_FILE = "empty_msgpack_unittest.d"

unittest:
	echo 'import msgpack; void main(){}' > $(MAIN_FILE)
	$(DMD) $(DFLAGS) -unittest -of$(LIB) $(SRCS) -run $(MAIN_FILE)
	rm $(MAIN_FILE)
