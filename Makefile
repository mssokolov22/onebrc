all:
	gcc -Wall -Wextra -O2 -g -fno-omit-frame-pointer src/mssokolov22.c -o onebrc

clean:
	rm -f onebrc
