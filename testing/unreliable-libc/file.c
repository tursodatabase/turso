#define _GNU_SOURCE
#include <sys/uio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <unistd.h>
#include <dlfcn.h>
#include <errno.h>
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <stdint.h>

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

	/* If no vectors or invalid count, forward directly. */
	if (iov == NULL || iovcnt <= 0) {
		return libc_pwritev(fd, iov, iovcnt, offset);
	}

    if (iovcnt > 0 && chance(short_write_probability)) {
        struct iovec iov_copy[iovcnt];
        memcpy(iov_copy, iov, sizeof(struct iovec) * iovcnt);

        /* Compute total bytes requested (guarding against overflow). */
        size_t total = 0;
        for (int i = 0; i < iovcnt; ++i) {
            size_t len = iov_copy[i].iov_len;
            if (len > 0 && total > SIZE_MAX - len) {
                total = SIZE_MAX;
                break;
            }
            total += len;
        }

        /* Only meaningful to short-write if total > 1. */
        if (total > 1) {
            /* Choose cutoff in [1, total-1] */
            size_t cutoff = 1 + (size_t)(lrand48() % (total - 1));
            size_t remaining = cutoff;

            /* Walk iov_copy and reduce lengths to exactly match cutoff.
             * After the cutoff is satisfied, following iov entries will be set to 0 length.
             */
            for (int i = 0; i < iovcnt; ++i) {
                size_t len = iov_copy[i].iov_len;
                if (len == 0) {
                    /* keep zero-length entries as zero, they do not consume remaining */
                    continue;
                }
                if (remaining == 0) {
                    /* drop the rest */
                    iov_copy[i].iov_len = 0;
                    continue;
                }
                if (len <= remaining) {
                    /* fully include this iovec */
                    remaining -= len;
                    /* keep iov_copy[i].iov_len as-is */
                } else {
                    /* partial include: shorten this iovec to remaining */
                    iov_copy[i].iov_len = remaining;
                    remaining = 0;
                }
            }

            /* Compute new iovcnt to avoid passing trailing zero-length iovecs */
            int new_iovcnt = iovcnt;
            while (new_iovcnt > 0 && iov_copy[new_iovcnt - 1].iov_len == 0) {
                new_iovcnt--;
            }
            /* Defensive: cutoff >= 1 so new_iovcnt should be > 0; This should always be true*/
            if (new_iovcnt > 0) {
                printf("%s: injecting fault SHORT WRITE cutoff=%zu of %zu (new_iovcnt=%d)\n",
                       __func__, cutoff, total, new_iovcnt);
                return libc_pwritev(fd, iov_copy, new_iovcnt, offset);
            }
        }
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
