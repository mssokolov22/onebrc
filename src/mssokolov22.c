#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <strings.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <pthread.h>


#define TABLE_SIZE (1 << 17)


typedef struct {
	unsigned char* name;
	size_t len;
	int16_t min, max;
	uint32_t count;
	int64_t cumm;
} Entry;

uint64_t collisions = 0;

typedef struct {
	unsigned char* addr;
	unsigned char* end;
	int first;
	int last;
	Entry* entries;
} Args;


size_t find_index(uint64_t h, unsigned char* name_ptr, size_t size, Entry* entries) {
    	size_t index = h & (TABLE_SIZE - 1);

    	while (entries[index].count != 0) {
        	if (entries[index].len == size
				&& memcmp(entries[index].name, name_ptr, size) == 0) {
        		break; 
		}
        	index = (index + 1) & (TABLE_SIZE - 1);
	}

	return index;
}

void add_entry(unsigned char* name_ptr, size_t index, size_t size, int temperature, Entry* entries) {
	entries[index].name = name_ptr;
	entries[index].len = size;
        entries[index].min = entries[index].max = entries[index].cumm = temperature;
        entries[index].count = 1; 
}


inline void update_entry(size_t index, int16_t temperature, Entry* entries) {
	entries[index].count += 1;
	entries[index].cumm += temperature;
	if (entries[index].max < temperature) entries[index].max = temperature;
	if (entries[index].min > temperature) entries[index].min = temperature;
}

inline unsigned char* parse(unsigned char* addr, int* temperature) {
	int sign = 1;
	
	if (*addr == '-') {
		sign = -1;
		addr++;
	}

	if (addr[1] == '.') {
		*temperature = (addr[0] * 10 + addr[2] - ('0' * 11)) * sign;
		return addr + 4;
	}
	
	*temperature = (addr[0] * 100 + addr[1] * 10 + addr[3] - ('0' * 111)) * sign;	
	return addr + 5;
}

void* one(void* input) {
	Args* a = (Args*) input;


	unsigned char* addr = a->addr;
	unsigned char* end = a->end;
	int first = a->first;
	int last = a->last;

	a->entries = calloc(TABLE_SIZE, sizeof(Entry));
	if (!first) {
		while (*addr != '\n') addr++;
		addr++;
	}

	if (!last)
		while (*end != '\n') end++;

	while (addr < end) {
		unsigned char* city_start = addr;
		uint64_t h = 0;
	
		while (*addr != ';') {
			h = h * 31 + *addr;
			addr++;
		}
		int len = addr - city_start;	

		// skip the ';'
		addr++;

		int temperature;
		addr = parse(addr, &temperature);
		
		size_t index = find_index(h, city_start, len, a->entries);

		if (a->entries[index].count == 0) {
			add_entry(city_start, index, len, temperature, a->entries);
		} else {
			update_entry(index, temperature, a->entries);
		}
		while (addr < end && (*addr == '\n' || *addr == '\r')) addr++;
	}

	return a->entries;
}


int main (int argc, char** argv) {
	setbuf(stdout, NULL);

	if (argc != 3) {
		perror("Incorrect number of arguments provided!\n");
		return 1;
	}

	const char* fn = argv[1];
	const char* wfn = argv[2];

	if (!fn || !wfn) { 
		perror("No file name/s provided!\n");
	}

	int fd = open(fn, O_RDONLY);

	if (fd < 0) {
		perror("Couldn't open file!\n");
		return 1;
	}

	struct stat sb;
	if (fstat(fd, &sb) < 0) {
		perror("Couldn't make a fstatistic!\n");
		return 1;
	}

	unsigned char* base = (unsigned char*) mmap(NULL, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);

	if (base == MAP_FAILED) {
		perror("Mmap failed!\n");
		return 1;
	}


	madvise(base, sb.st_size, MADV_SEQUENTIAL);

	size_t nthreads = 8;
	pthread_t threads[nthreads];
	size_t chunk_size = sb.st_size / nthreads;
	Args args[nthreads]; 
	
	for (size_t i = 0; i < nthreads; i++) {
		args[i].addr = base + i * chunk_size;
		if (i == nthreads - 1) {
			args[i].end = base + sb.st_size;
			args[i].last = 1;
		} else {
			args[i].end = base + (i + 1) * chunk_size;
			args[i].last = 0;
		}
		if (i == 0) args[i].first = 1;
		else args[i].first = 0; 
		pthread_create(&threads[i], NULL, one, &args[i]);
	}

	for (size_t i = 0; i < nthreads; i++) {
		pthread_join(threads[i], NULL);
	}

	Entry entries[TABLE_SIZE];
	memset(entries, 0, sizeof(Entry) * TABLE_SIZE);

	for (size_t t = 0; t < nthreads; t++) {
		for (size_t i = 0; i < TABLE_SIZE; i++) {
			Entry* entry = &args[t].entries[i];
			if (entry->count == 0) continue;

			size_t h = 0;
			for (size_t k = 0; k < entry->len; k++)
				h = h * 31 + entry->name[k];

			size_t index = find_index(h, entry->name, entry->len, entries);

			if (entries[index].count == 0) {
				entries[index] = *entry;
			}
			else {
				entries[index].count += entry->count;
				entries[index].cumm += entry->cumm;
				if (entries[index].max < entry->max) entries[index].max = entry->max;
				if (entries[index].min > entry->min) entries[index].min = entry->min;
			}
		}
	}


	// printf("%lu\n", collisions);
	FILE* wfptr = fopen(wfn, "w");
	if (wfptr == NULL) {
		perror("Write file open seg!\n");
		return 1;
	}

	for (size_t i = 0; i < TABLE_SIZE; i++) {
		if (entries[i].count > 0)
			fprintf(wfptr, "%.*s;%.1f;%.1f;%.1f\n",
					(int) entries[i].len, entries[i].name,
					entries[i].min / 10.0f,
					entries[i].max / 10.0f,
					entries[i].cumm / (entries[i].count * 10.0f));
	}

	munmap(base, sb.st_size);
	close(fd);
	fclose(wfptr);

}
