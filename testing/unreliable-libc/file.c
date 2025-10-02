#define _GNU_SOURCE
#include <sys/uio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unistd.h>
#include <dlfcn.h>
#include <errno.h>
#include <stdio.h>
#include <time.h>

static double probabilities[] = {
	[ENOSPC] = 0.01,
	[EIO] = 0.01,
};

static double short_write_probability = 0.05; // 5% chance of a short write

static bool chance(double probability)
{
	double event = drand48();
	return event < probability;
}

static bool inject_fault(int error)
{
	double probability = probabilities[error];
	if (chance(probability)) {
		errno = error;
		return true;
	}
	errno = 0;
	return false;
}

static ssize_t (*libc_pwrite) (int, const void *, size_t, off_t);

ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset)
{
	if (libc_pwrite == NULL) {
		libc_pwrite = dlsym(RTLD_NEXT, "pwrite");
	}

	if (count > 1 && chance(short_write_probability)) {
        size_t short_count = 1 + (lrand48() % (count - 1));
        printf("%s: injecting fault SHORT WRITE (requesting %zu instead of %zu)\n", __func__, short_count, count);
        return libc_pwrite(fd, buf, short_count, offset);
    }

	if (inject_fault(ENOSPC)) {
		printf("%s: injecting fault NOSPC\n", __func__);
		return -1;
	}
	if (inject_fault(EIO)) {
		printf("%s: injecting fault EIO\n", __func__);
		return -1;
	}
	return libc_pwrite(fd, buf, count, offset);
}

static ssize_t (*libc_pwritev) (int, const struct iovec *, int, off_t);

ssize_t pwritev(int fd, const struct iovec *iov, int iovcnt, off_t offset)
{
	if (libc_pwritev == NULL) {
		libc_pwritev = dlsym(RTLD_NEXT, "pwritev");
	}
	if (inject_fault(ENOSPC)) {
		printf("%s: injecting fault NOSPC\n", __func__);
		return -1;
	}
	if (inject_fault(EIO)) {
		printf("%s: injecting fault EIO\n", __func__);
		return -1;
	}
	return libc_pwritev(fd, iov, iovcnt, offset);
}

static int (*libc_fsync) (int);

int fsync(int fd)
{
	if (libc_fsync == NULL) {
		libc_fsync = dlsym(RTLD_NEXT, "fsync");
	}
	if (inject_fault(ENOSPC)) {
		printf("%s: injecting fault NOSPC\n", __func__);
		return -1;
	}
	if (inject_fault(EIO)) {
		printf("%s: injecting fault EIO\n", __func__);
		return -1;
	}
	return libc_fsync(fd);
}

__attribute__((constructor))
static void init(void)
{
	char *env_seed = getenv("UNRELIABLE_LIBC_SEED");
	long seedval;
	if (!env_seed) {
		seedval = time(NULL);
	} else {
		seedval = atoi(env_seed);
	}
	printf("UNRELIABLE_LIBC_SEED = %ld\n", seedval);
	srand48(seedval);
}
