CFLAGS=-std=c99 -O3
JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64/jre

.PHONY : native

native : bin/native/reedsolomon.so

bin/native/reedsolomon.so : src/warrenfalk/reedsolomon/ReedSolomonNative.c
	mkdir -p `dirname $@`
	$(CC) $(CFLAGS) -fPIC -Wall -D_JNI_IMPLEMENTATION_ -Iinclude/gen -Iinclude -I$(JAVA_HOME)/include -I$(JAVA_HOME)/../include -shared -o $@ $<