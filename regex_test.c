#include <sys/types.h>
#include <regex.h>
#include <stdio.h>

#define MAX_MATCHES 5 //The maximum number of matches allowed in a single string

void match(regex_t *pexp, char *sz) {
	regmatch_t matches[MAX_MATCHES]; //A list of the matches in the string (a list of 1)
	//Compare the string to the expression
	//regexec() returns 0 on match, otherwise REG_NOMATCH
	if (regexec(pexp, sz, MAX_MATCHES, matches, 0) == 0) {
		printf("\"%s\" matches characters %d - %d\n", sz, matches[0].rm_so, matches[0].rm_eo);
		printf("\"%s\" matches characters %d - %d\n", sz, matches[1].rm_so, matches[1].rm_eo);
	} else {
		printf("\"%s\" does not match\n", sz);
	}
}
 
int main() {
	int rv;
	regex_t exp; //Our compiled expression
	//1. Compile our expression.
	//Our regex is "-?[0-9]+(\\.[0-9]+)?". I will explain this later.
	//REG_EXTENDED is so that we can use Extended regular expressions
	rv = regcomp(&exp, "(txt)", REG_EXTENDED);
	if (rv != 0) {
		printf("regcomp failed with %d\n", rv);
	}
	//2. Now run some tests on it
	match(&exp, "osvaldo.txt.txt");
	match(&exp, "osvaldo.txt.osvaldo.txt");
	match(&exp, "0.0");
	match(&exp, "10.1");
	match(&exp, "-10.1");
	match(&exp, "a");
	match(&exp, "a.1");
	match(&exp, "0.a");
	match(&exp, "0.1a");
	match(&exp, "hello");
	//3. Free it
	regfree(&exp);
	return 0;
}
