#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <regex.h>

char enter_reverse_mode[] = "\33[7m";
char exit_reverse_mode[] = "\33[0m";

int main(int argc, char **argv)
{
  const char *pattern;
  int ec;
  regex_t expr;
  regmatch_t rm;
  char buf[12];
  size_t offset, length;
  int flags;
  assert(argc == 2);
  pattern = argv[1];
  if ((ec = regcomp(&expr, pattern, 0)) != 0) {
    char str[256];
    regerror(ec, &expr, str, sizeof str);
    fprintf(stderr, "%s: %s\n", pattern, str);
    return EXIT_FAILURE;
  }
  flags = 0;
  while (fgets(buf, sizeof buf, stdin)) {
    /* Find the end of the buffer */
    length = strcspn(buf, "\n");
    /* Check for beginning and end of line. */
    if (flags & REG_NOTEOL) {
            /* If the last line read was a partial line, then we are
             * not at the beginning of the line. */
      flags |= REG_NOTBOL;
      if (buf[length] == '\n')
        flags &= ~REG_NOTEOL;
    }
    else if (buf[length] != '\n') {
      /* We've read a partial line. */
      flags = REG_NOTEOL;
    }
    else {
      /* We have a complete line. */
      flags = 0;
    }
    /* get rid of any newline character */
    buf[length] = '\0';
    /* start at beginning of the buffer */
    offset = 0;
    while (regexec(&expr, buf + offset, 1, &rm, flags) == 0) {
      assert(rm.rm_so >= 0);
      /* we're not smart enough to support empty matches. */
      assert(rm.rm_eo > rm.rm_so);
      /* print the portion which precedes the match, then the match */
      printf("%.*s%s%.*s%s",
        rm.rm_so, buf + offset,
        enter_reverse_mode,
        rm.rm_eo - rm.rm_so, buf + offset + rm.rm_so,
        exit_reverse_mode);
      /* start next match at the end of this one. */
      offset += rm.rm_eo;
      /* we're no longer at the beginning of the line */
      flags |= REG_NOTBOL;
    }
    /* print remainder of the line */
    printf("%s", buf + offset);
    /* print a newline if we're at the end of a line */
    if (!(flags & REG_NOTEOL))
      putchar('\n');
  }
  return EXIT_SUCCESS;
}
