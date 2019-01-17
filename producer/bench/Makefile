OBJS		= kafka_p.o src/config.o src/server.o src/main.o
BIN			= example_exe
CXX			= g++
CXXFLAGS	= -O3 -g -ggdb3 -std=c++11
BIN_DIR     = ./bin
OBJ_DIR		= ./obj
SRC_DIR		= ./src

INC			= -I./src \
			  -I./third \
			  -I../ \
			  -I../../opensource/include \
			  -I/usr/local \

LIB			= -lpthread \
			  ./third/lib/libiniparser.a \
			  -L../../opensource/lib \
			  -lrt -lrdkafka \


all: dir $(addprefix $(BIN_DIR)/,$(BIN))

dir:
	@-mkdir -p $(OBJ_DIR)
	@-mkdir -p $(OBJ_DIR)/src
	@-mkdir -p $(BIN_DIR)

$(OBJ_DIR)/src/%.o:src/%.cpp
	$(CXX) $(CXXFLAGS) $(INC) -o $@ -c $<

$(OBJ_DIR)/%.o:../%.cpp
	$(CXX) $(CXXFLAGS) $(INC) -o $@ -c $<

$(BIN_DIR)/$(BIN) : $(addprefix $(OBJ_DIR)/,${OBJS})
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIB)

clean:
	@rm -rf $(addprefix $(BIN_DIR)/,$(BIN)) $(OBJ_DIR)/*
	@rm -rf $(BIN_DIR)/$(BIN) $(OBJ_DIR)
