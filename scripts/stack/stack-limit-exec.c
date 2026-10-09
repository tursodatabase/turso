// Runs a command with the stack size limit set to KIB kibibytes.
//
// Usage: stack-limit-exec KIB COMMAND [ARG]...
//
// stack-profile.sh starts this under lldb instead of setting `ulimit -s` in
// the shell that runs lldb: lldb's own debug server needs more stack than a
// small limit allows, and lldb cannot debug a system shell on macOS.
#include <stdio.h>
#include <stdlib.h>
#include <sys/resource.h>
#include <unistd.h>

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: %s KIB COMMAND [ARG]...\n", argv[0]);
        return 2;
    }
    struct rlimit limit;
    if (getrlimit(RLIMIT_STACK, &limit) != 0) {
        perror("getrlimit");
        return 2;
    }
    limit.rlim_cur = (rlim_t)strtoull(argv[1], NULL, 10) * 1024;
    if (setrlimit(RLIMIT_STACK, &limit) != 0) {
        perror("setrlimit");
        return 2;
    }
    execvp(argv[2], &argv[2]);
    perror("execvp");
    return 2;
}
