/* 
 * File:   main.c
 * Author: osvaldoandrade
 *
 * Created on 8 de Março de 2012, 19:14
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <regex.h>
#include <string.h>
#include <assert.h>
#include <fcntl.h>
#include <unistd.h>
#include <ctype.h>
#include <xlocale.h>

#define NUL '\0'
#define MAX_LINE 10000

#define OK                   0
#define INSUFFICIENT_MEMORY  1
#define FILE_CREATION_ERROR  2
#define FILE_WRITE_ERROR     3


#define point_ms ((struct read_write_compare_ms *) pointer)
#define buf_ms   ((struct numbered_record_ms *) buffer)

struct read_write_compare_ms {
    FILE *unsorted_file;
    FILE *sorted_file;
    int (*read)(FILE *, void *, void *);
    int (*write)(FILE *, void *, void *);
    int (*compare)(void *, void *, void *);
    void *pointer;
    unsigned long record_number;
};

struct numbered_record_ms {
    unsigned long number;
    char record[1];
};

struct record_in_memory_helper {
    struct record_in_memory_helper *next;
    char record[1];
};

struct compare_info_helper {
    int (*compare)(void *, void *, void *);
    void *pointer;
};

static void
scan_dir(char* input_dir, char* output_file);

static int
parse_file(char* filename, char* output_file);

static int
configure(char** input_dir, char** output_file, char** ext, int argc, char** argv);

static void
print_usage();

static int
extract_file_ext_regex(char** destin, char* str);

static void
release();

static char*
trim(char* str);

static unsigned
get_number(char *s);

static void
invalid_column_number(void);

static int
write_record(FILE *f, void *buffer, void *pointer);

static int
read_record(FILE *f, void *buffer, void *pointer);

static int
compare(void *p, void *q, void *pointer);

static int
compare_case(void *p, void *q, void *pointer);

static int
remove_same_words(FILE* input, FILE* output);

static int
read_record_ms(FILE *f, void *buffer, void *pointer);

static int
write_record_ms(FILE *f, void *buffer, void *pointer);

static int
compare_records_ms(void *p, void *q, void *pointer);

int
stable_merge_sort(FILE *unsorted_file, FILE *sorted_file,
        int (*read)(FILE *, void *, void *),
        int (*write)(FILE *, void *, void *),
        int (*compare)(void *, void *, void *), void *pointer,
        unsigned max_record_size, unsigned long block_size,
        unsigned long *record_count);

static void
free_memory_blocks_helper(struct record_in_memory_helper *first);

static int
compare_records_helper(void *p, void *q, void *pointer);

int merge_sort(FILE *unsorted_file, FILE *sorted_file,
        int (*read)(FILE *, void *, void *),
        int (*write)(FILE *, void *, void *),
        int (*compare)(void *, void *, void *), void *pointer,
        unsigned max_record_size, unsigned long block_size, unsigned long *pcount);

void *sort_linked_list(void *p, unsigned index,
        int (*compare)(void *, void *, void *), void *pointer, unsigned long *pcount);

static regex_t ext_regex;
static regex_t word_regex;
static char* file_ext_regex;
static FILE *f_output_file;
static FILE *unsorted_file;
unsigned first_column = 0;
unsigned last_column = MAX_LINE - 1;

/*
 * 
 */
int
main(int argc, char** argv) {

    char* input_dir;
    char* output_file;
    char* ext;

    //Configura variaveis de entrada
    if (configure(&input_dir, &output_file, &ext, argc, argv) != 0) return;

    //Realiza leitura recursiva no diretorio informado e aplica o parse utilizando expressoes regulares
    scan_dir(input_dir, output_file);

    fseek(unsorted_file, 0, SEEK_SET);

    FILE* sorted_file = NULL;

    if ((sorted_file = tmpfile()) == NULL) {
        perror("Erro ao abrir arquivo temporario");
        exit(1);
    };

    int n = stable_merge_sort(unsorted_file, sorted_file, read_record, write_record,
            compare, NULL, MAX_LINE + 1, 150000 / (MAX_LINE + 5), NULL);
    if (n != 0) {
        static char *ERROR[] = {
            "INSUFFICIENT MEMORY",
            "FILE CREATION ERROR",
            "FILE WRITE ERROR"
        };
        fprintf(stderr, "SORT: %s\n", ERROR[n - 1]);
        return 1;
    }


    FILE* clean_sorted_file;
    if ((clean_sorted_file = tmpfile()) == NULL) {
        perror("Erro ao abrir arquivo temporario");
        exit(1);
    };

    fseek(sorted_file, 0, SEEK_SET);
    n = remove_same_words(sorted_file, clean_sorted_file);
    if (n != 0) {
        perror("Erro ao remover palavras iguais");
    }
    
    
    fseek(clean_sorted_file, 0, SEEK_SET);
     n = stable_merge_sort(clean_sorted_file, f_output_file, read_record, write_record,
             compare_case, NULL, MAX_LINE + 1, 150000 / (MAX_LINE + 5), NULL);
     if (n != 0) {
         static char *ERROR[] = {
             "INSUFFICIENT MEMORY",
             "FILE CREATION ERROR",
             "FILE WRITE ERROR"
         };
         fprintf(stderr, "SORT: %s\n", ERROR[n - 1]);
         return 1;
     }  
     fclose(clean_sorted_file);
    fclose(unsorted_file);
    fclose(sorted_file);
    fclose(f_output_file);


    //Libera recursos alocados 
    release();
    return (EXIT_SUCCESS);
}

/**
 *Configura as váriaveis locais utilizadas pela aplicacao a partir dos argumentos de entrada.
 */
static int
configure(char** input_dir, char** output_file, char** ext, int argc, char** argv) {
    int err = 0;
    if (argc >= 5) {
        int i = 1;

        for (i = 1; i < argc; i++) {
            if (strcmp(argv[i], "-d") == 0) {
                if (argv[++i] == NULL) {
                    print_usage();
                    exit(1);
                }
                *input_dir = malloc(strlen(argv[i]));
                strncpy(*input_dir, argv[i], strlen(argv[i]));
            }

            if (strcmp(argv[i], "-o") == 0) {
                if (argv[++i] == NULL) {
                    print_usage();
                    exit(1);
                }
                *output_file = malloc(strlen(argv[i]));
                strncpy(*output_file, argv[i], strlen(argv[i]));

                if ((f_output_file = fopen(*output_file, "ab+")) == NULL) {
                    perror("Erro ao abrir arquivo de saida");
                    exit(1);
                };

                if ((unsorted_file = tmpfile()) == NULL) {
                    perror("Erro ao abrir arquivo de saida");
                    exit(1);
                };
            }

            if (strcmp(argv[i], "-e") == 0) {
                if (argv[++i] == NULL) {
                    print_usage();
                    exit(1);
                }
                *ext = malloc(strlen(argv[i]));
                strncpy(*ext, argv[i], strlen(argv[i]));

                err = extract_file_ext_regex(&file_ext_regex, *ext);
            }
        }

        if (file_ext_regex == NULL) {
            *ext = malloc(8);
            strncpy(*ext, "txt:text", 8);
            err = extract_file_ext_regex(&file_ext_regex, *ext);
            printf("Filtro utilizado: %s", file_ext_regex);

        }
        if (*input_dir == NULL) {
            print_usage();
            exit(1);
        }

    } else {
        print_usage();
        err = 1;
    }

    return err;
}

/**
 *Realizada a leitura recursiva no diretorio informado input_dir e para cada arquivo encontrado, é feita 
 *uma leitura no conteúdo do arquvo em busca de palavras que combinem com word_regex. 
 */
static void
scan_dir(char* input_dir, char* output_file) {

    DIR *dirp;
    struct dirent *dp;
    int err;
    char err_msg[200];

    dirp = opendir(input_dir);
    chdir(input_dir);

    if ((err = regcomp(&ext_regex, file_ext_regex, REG_EXTENDED)) != 0) {
        regerror(err, &ext_regex, err_msg, 200);
        printf("Erro ao compilar expresao regular '%s': %s. \n", ".", err_msg);
        return;
    };

    while ((dp = readdir(dirp)) != NULL) {

        if (strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0) {
            continue;
        }

        struct stat buf_tmp;
        char * path_name = strdup(input_dir);
        path_name = realloc(path_name, strlen(input_dir) + strlen(dp->d_name) + 2);
        strncat(path_name, "/", 1);
        strncat(path_name, dp->d_name, strlen(dp->d_name));

        if (stat(path_name, &buf_tmp) == -1) {
            printf("Erro ao tentar realizar stat em: %s \n", path_name);
            return;
        }

        if (S_ISDIR(buf_tmp.st_mode) && (strcmp(dp->d_name, ".") != 0) && strcmp(dp->d_name, "..")) {
            //printf("%s is a directory\n  ", path_name);
            scan_dir(path_name, output_file);
        } else if (S_ISREG(buf_tmp.st_mode) && (regexec(&ext_regex, dp->d_name, 0, NULL, 0) == 0)) {
            //printf("%s is a regular file according to %s\n", path_name, file_ext_regex);
            int err = parse_file(path_name, output_file);
        }
    }

    (void) closedir(dirp);
}

/**
 *Para cada arquivo identificado a partir da expressao regular ext_regex, realizar o parse, efetua a leitura 
 *em blocos de 4096 bytes, e aplica um novo filtro utilizando word_regex. Os resultados combinados (match) 
 *sao escritos em output_file (APPEND).
 */
static int
parse_file(char* path_name, char* output_file) {
    int fd;
    int n;
    char buf_tmp[4096];
    int offset, err;

    if ((err = regcomp(&word_regex, "( )*[a-zA-Z0-9]+( )*", REG_EXTENDED)) != 0) {
        return -1;
    }

    fd = open(path_name, O_RDONLY);
    if (fd == -1) {
        perror("Erro ao abrir arquivo");
        return -1;
    }

    while ((n = read(fd, buf_tmp, 4096)) > 0) {
        offset = 0;
        regmatch_t rm;
        char* word = NULL;
        int flags = 0;
        while (regexec(&word_regex, buf_tmp + offset, 1, &rm, flags) == 0) {
            assert(rm.rm_so >= 0);
            assert(rm.rm_eo > rm.rm_so);
            int begin = (int) rm.rm_so;
            int end = (int) rm.rm_eo;
            int len = end - begin;
            int w = 0;
            int i = 0;

            char* tmp = buf_tmp + offset;
            word = malloc(len + 1);
            for (i = begin; i < end; i++) {
                word[w] = tmp[i];
                w++;
            }
            word[w] = 0;

            offset += rm.rm_eo;

            word = trim(word);

            char* tmp_str = malloc(strlen(word) + 1);
            strncat(tmp_str, word, strlen(word));
            strncat(tmp_str, "\n", 1);

            //write(STDOUT_FILENO, tmp_str, strlen(tmp_str));	
            fwrite(tmp_str, strlen(tmp_str), 1, unsorted_file);
            /* Alterna flags pois nao estamos mais no inicio da linha */
            flags |= REG_NOTBOL;
        }

    }

    if (n < 0)
        perror("Erro ao ler arquivo");

    close(fd);


    return 0;
}

static void
release() {
    regfree(&word_regex);
    regfree(&ext_regex);
}

/**
 *Funcao responsavel por quebrar os filtros informados através do argumento de entrada (ext) e popular a string 
 *destin dinamicamente.
 */
static int
extract_file_ext_regex(char** destin, char* ext) {
    char* result;
    int err = 0;

    result = strtok(ext, ":");
    *destin = malloc(strlen(result) + 3);
    strncat(*destin, "(\\.", 4);
    strncat(*destin, result, strlen(result));
    strncat(*destin, ")", 2);

    while (result != NULL) {
        result = strtok(NULL, ":");
        if (result != NULL) {
            char * tmp = malloc(strlen(result) + 4);
            strncat(tmp, "|(\\.", 5);
            strncat(tmp, result, strlen(result));
            strncat(tmp, ")", 2);
            strncat(*destin, tmp, strlen(tmp));
            free(tmp);
        }
    }
    return err;
}

/**
 *Imprime as opcoes de uso do programa.
 */
static void
print_usage() {
    printf("\n"
            "Lista de parametros: \n"
            "\t -d Caminho absoluto do diretorio de entrada \n"
            "\t -o Arquivo de saida \n"
            "\t -e Extensoes de arquivos a serem filtradas\n"
            "\n"
            "Exemplo de uso: \n\twordharvest -d /home/osvaldo/wordharvest -e text:txt:asc -o ./harvest.dic\n\n"
            );
}

static char *
trim(char *str) {
    char *ibuf = str, *obuf = str;
    int i = 0, cnt = 0;

    if (str) {

        for (ibuf = str; *ibuf && isspace(*ibuf); ++ibuf)
            ;
        if (str != ibuf)
            memmove(str, ibuf, ibuf - str);

        while (*ibuf) {
            obuf[i++] = *ibuf++;
        }
        obuf[i] = NUL;

        while (--i >= 0) {
            if (!isspace(obuf[i]))
                break;
        }
        obuf[++i] = NUL;
    }
    return str;
}

static int
compare(void *p, void *q, void *pointer) {
    unsigned i = 0;
#define pp ((unsigned char *) p)
#define qq ((unsigned char *) q)

    return strcmp(pp, qq);
}

static int
compare_case(void *p, void *q, void *pointer) {
    unsigned i = 0;
#define pp ((unsigned char *) p)
#define qq ((unsigned char *) q)

    //locale_t locale = newlocale(LC_NUMERIC_MASK, "pt_BR.ISO8859-1", NULL);

    //return strcasecmp(pp,qq);
    return strcasecmp(pp, qq);

    //freelocale(locale);
}

static int
read_record(FILE *f, void *buffer, void *pointer) {
    int n = 0;
    int c;
    if (feof(f) || (c = getc(f)) == EOF) return 0;
    while (c != '\n' && c != EOF) {
        if (n < MAX_LINE)
            ((char *) buffer)[n++] = c;
        else {
            fputs("SORT: Input line is too long\n", stderr);
            exit(1);
        }
        c = getc(f);
    }
    ((char *) buffer)[n++] = 0;
    return n;
}

static int
write_record(FILE *f, void *buffer, void *pointer) {
    return fputs((char *) buffer, f) != EOF && putc('\n', f) != EOF;
}

static void
invalid_column_number(void) {
    fputs("SORT: Invalid column number\n", stderr);
    exit(1);
}

static unsigned
get_number(char *s) {
    unsigned n = 0;
    while (*s != 0) {
        if (isdigit(*s))
            n = 10 * n + *s++ -'0';
        else
            invalid_column_number();
        if (n > MAX_LINE - 1)
            invalid_column_number();
    }
    return n;
}

static int
remove_same_words(FILE* input, FILE* ouput) {
    int offset = 0;
    int read_bytes = 0;
    char* current_word = malloc(MAX_LINE);
    char* last_word = malloc(MAX_LINE);
    fseek(input, 0L, SEEK_SET);
    while (read_bytes = getline(&current_word, &offset, input) > 0) {
        if (strcmp(current_word, last_word) != 0) {
            fwrite(last_word, strlen(last_word), 1, ouput);
        }

        last_word = strndup(current_word, strlen(current_word));

        offset += read_bytes;
    }

    return 0;
}

static int
read_record_ms(FILE *f, void *buffer, void *pointer) {
    int n;
    if (f == point_ms->unsorted_file)
        buf_ms->number = point_ms->record_number++;
    else
        fread(&buf_ms->number, sizeof (unsigned long), 1, f);
    n = (*point_ms->read)(f, buf_ms->record, point_ms->pointer);
    if (n != 0)
        n += sizeof (unsigned long);
    return n;
}

static int
write_record_ms(FILE *f, void *buffer, void *pointer) {
    if (f != point_ms->sorted_file &&
            fwrite(&buf_ms->number, sizeof (unsigned long), 1, f) == 0) {
        return 0;
    }
    return (*point_ms->write)(f, buf_ms->record, point_ms->pointer);
}

static int
compare_records_ms(void *p, void *q, void *pointer) {
#define pp_ms ((struct numbered_record_ms *) p)
#define qq_ms ((struct numbered_record_ms *) q)
    int n = (*point_ms->compare)(pp_ms->record, qq_ms->record, point_ms->pointer);
    if (n == 0)
        n = pp_ms->number < qq_ms->number ? -1 : 1;
    return n;
}

int
stable_merge_sort(FILE *unsorted_file, FILE *sorted_file,
        int (*read)(FILE *, void *, void *),
        int (*write)(FILE *, void *, void *),
        int (*compare)(void *, void *, void *), void *pointer,
        unsigned max_record_size, unsigned long block_size,
        unsigned long *record_count) {
    struct read_write_compare_ms rwc;
    rwc.unsorted_file = unsorted_file;
    rwc.sorted_file = sorted_file;
    rwc.read = read;
    rwc.write = write;
    rwc.compare = compare;
    rwc.pointer = pointer;
    rwc.record_number = 0L;
    return merge_sort(unsorted_file, sorted_file, read_record_ms, write_record_ms,
            compare_records_ms, &rwc, sizeof (unsigned long) +max_record_size,
            block_size, record_count);
}

static void
free_memory_blocks_helper(struct record_in_memory_helper *first) {
    while (first != NULL) {
        struct record_in_memory_helper *next = first->next;
        free(first);
        first = next;
    }
}

static int
compare_records_helper(void *p, void *q, void *pointer) {
#define pp_helper ((struct record_in_memory_helper *) p)
#define qq_helper ((struct record_in_memory_helper *) q)
#define point ((struct compare_info_helper *) pointer)
    return (*point->compare)(pp_helper->record, qq_helper->record, point->pointer);
}

int
merge_sort(FILE *unsorted_file, FILE *sorted_file,
        int (*read)(FILE *, void *, void *),
        int (*write)(FILE *, void *, void *),
        int (*compare)(void *, void *, void *), void *pointer,
        unsigned max_record_size, unsigned long block_size, unsigned long *pcount) {

    struct tape {
        FILE *fp;
        unsigned long count;
    };
    struct tape source_tape[2];
    char *record[2];
    /* aloca memoria*/
    if ((record[0] = malloc(max_record_size)) == NULL)
        return INSUFFICIENT_MEMORY;
    if ((record[1] = malloc(max_record_size)) == NULL) {
        free(record[0]);
        return INSUFFICIENT_MEMORY;
    }
    source_tape[0].fp = tmpfile();
    source_tape[0].count = 0L;
    if (source_tape[0].fp == NULL) {
        free(record[0]);
        free(record[1]);
        return FILE_CREATION_ERROR;
    }
    source_tape[1].fp = tmpfile();
    source_tape[1].count = 0L;
    if (source_tape[1].fp == NULL) {
        fclose(source_tape[0].fp);
        free(record[0]);
        free(record[1]);
        return FILE_CREATION_ERROR;
    }
    /* read blocks, sort them in memory, and write the alternately to */
    /* tapes 0 and 1 */
    {
        struct record_in_memory_helper *first = NULL;
        unsigned long block_count = 0;
        unsigned destination = 0;
        struct compare_info_helper comp;
        comp.compare = compare;
        comp.pointer = pointer;
        while (1) {
            int record_size = (*read)(unsorted_file, record[0], pointer);
            if (record_size > 0) {
                struct record_in_memory_helper *p = (struct record_in_memory_helper *)
                        malloc(sizeof (struct record_in_memory_helper *) +record_size);
                if (p == NULL) {
                    fclose(source_tape[0].fp);
                    fclose(source_tape[1].fp);
                    free(record[0]);
                    free(record[1]);
                    free_memory_blocks_helper(first);
                    return INSUFFICIENT_MEMORY;
                }
                p->next = first;
                memcpy(p->record, record[0], record_size);
                first = p;
                block_count++;
            }
            if (block_count == block_size || record_size == 0 && block_count != 0) {
                first = sort_linked_list(first, 0, compare_records_helper, &comp, NULL);
                while (first != NULL) {
                    struct record_in_memory_helper *next = first->next;
                    if ((*write)(source_tape[destination].fp, first->record,
                            pointer) == 0) {
                        fclose(source_tape[0].fp);
                        fclose(source_tape[1].fp);
                        free(record[0]);
                        free(record[1]);
                        free_memory_blocks_helper(first);
                        return FILE_WRITE_ERROR;
                    }
                    source_tape[destination].count++;
                    free(first);
                    first = next;
                }
                destination ^= 1;
                block_count = 0;
            }
            if (record_size == 0)
                break;
        }
    }
    if (sorted_file == unsorted_file)
        rewind(unsorted_file);
    rewind(source_tape[0].fp);
    rewind(source_tape[1].fp);
    /* delete the unsorted file here, if required (see instructions) */
    /* handle case where memory sort is all that is required */
    if (source_tape[1].count == 0L) {
        fclose(source_tape[1].fp);
        source_tape[1] = source_tape[0];
        source_tape[0].fp = sorted_file;
        while (source_tape[1].count-- != 0L) {
            (*read)(source_tape[1].fp, record[0], pointer);
            if ((*write)(source_tape[0].fp, record[0], pointer) == 0) {
                fclose(source_tape[1].fp);
                free(record[0]);
                free(record[1]);
                return FILE_WRITE_ERROR;
            }
        }
    } else {
        /* merge tapes, two by two, until every record is in source_tape[0] */
        while (source_tape[1].count != 0L) {
            unsigned destination = 0;
            struct tape destination_tape[2];
            destination_tape[0].fp = source_tape[0].count <= block_size ?
                    sorted_file : tmpfile();
            destination_tape[0].count = 0L;
            if (destination_tape[0].fp == NULL) {
                fclose(source_tape[0].fp);
                fclose(source_tape[1].fp);
                free(record[0]);
                free(record[1]);
                return FILE_CREATION_ERROR;
            }
            destination_tape[1].fp = tmpfile();
            destination_tape[1].count = 0L;
            if (destination_tape[1].fp == NULL) {
                if (destination_tape[0].fp != sorted_file)
                    fclose(destination_tape[0].fp);
                fclose(source_tape[0].fp);
                fclose(source_tape[1].fp);
                free(record[0]);
                free(record[1]);
                return FILE_CREATION_ERROR;
            }
            (*read)(source_tape[0].fp, record[0], pointer);
            (*read)(source_tape[1].fp, record[1], pointer);
            while (source_tape[0].count != 0L) {
                unsigned long count[2];
                count[0] = source_tape[0].count;
                if (count[0] > block_size)
                    count[0] = block_size;
                count[1] = source_tape[1].count;
                if (count[1] > block_size)
                    count[1] = block_size;
                while (count[0] + count[1] != 0) {
                    unsigned select = count[0] == 0 ? 1 : count[1] == 0 ? 0 :
                            compare(record[0], record[1], pointer) < 0 ? 0 : 1;
                    if ((*write)(destination_tape[destination].fp, record[select],
                            pointer) == 0) {
                        if (destination_tape[0].fp != sorted_file)
                            fclose(destination_tape[0].fp);
                        fclose(destination_tape[1].fp);
                        fclose(source_tape[0].fp);
                        fclose(source_tape[1].fp);
                        free(record[0]);
                        free(record[1]);
                        return FILE_WRITE_ERROR;
                    }
                    if (source_tape[select].count > 1L)
                        (*read)(source_tape[select].fp, record[select], pointer);
                    source_tape[select].count--;
                    count[select]--;
                    destination_tape[destination].count++;
                }
                destination ^= 1;
            }
            fclose(source_tape[0].fp);
            fclose(source_tape[1].fp);
            if (fflush(destination_tape[0].fp) == EOF ||
                    fflush(destination_tape[1].fp) == EOF) {
                if (destination_tape[0].fp != sorted_file)
                    fclose(destination_tape[0].fp);
                fclose(destination_tape[1].fp);
                free(record[0]);
                free(record[1]);
                return FILE_WRITE_ERROR;
            }
            rewind(destination_tape[0].fp);
            rewind(destination_tape[1].fp);
            memcpy(source_tape, destination_tape, sizeof (source_tape));
            block_size <<= 1;
        }
    }
    fclose(source_tape[1].fp);
    if (pcount != NULL)
        *pcount = source_tape[0].count;
    free(record[0]);
    free(record[1]);
    return OK;
}

void *
sort_linked_list(void *p, unsigned index,
        int (*compare)(void *, void *, void *), void *pointer, unsigned long *pcount) {
    unsigned base;
    unsigned long block_size;

    struct record {
        struct record * next[1];
        /* other members not directly accessed by this function */
    };

    struct tape {
        struct record *first, *last;
        unsigned long count;
    } tape[4];

    /* Distribute the records alternately to tape[0] and tape[1]. */

    tape[0].count = tape[1].count = 0L;
    tape[0].first = NULL;
    base = 0;
    while (p != NULL) {
        struct record *next = ((struct record *) p)->next[index];
        ((struct record *) p)->next[index] = tape[base].first;
        tape[base].first = ((struct record *) p);
        tape[base].count++;
        p = next;
        base ^= 1;
    }

    /* If the list is empty or contains only a single record, then */
    /* tape[1].count == 0L and this part is vacuous.               */

    for (base = 0, block_size = 1L; tape[base + 1].count != 0L;
            base ^= 2, block_size <<= 1) {
        int dest;
        struct tape *tape0, *tape1;
        tape0 = tape + base;
        tape1 = tape + base + 1;
        dest = base ^ 2;
        tape[dest].count = tape[dest + 1].count = 0;
        for (; tape0->count != 0; dest ^= 1) {
            unsigned long n0, n1;
            struct tape *output_tape = tape + dest;
            n0 = n1 = block_size;
            while (1) {
                struct record *chosen_record;
                struct tape *chosen_tape;
                if (n0 == 0 || tape0->count == 0) {
                    if (n1 == 0 || tape1->count == 0)
                        break;
                    chosen_tape = tape1;
                    n1--;
                } else if (n1 == 0 || tape1->count == 0) {
                    chosen_tape = tape0;
                    n0--;
                } else if ((*compare)(tape0->first, tape1->first, pointer) > 0) {
                    chosen_tape = tape1;
                    n1--;
                } else {
                    chosen_tape = tape0;
                    n0--;
                }
                chosen_tape->count--;
                chosen_record = chosen_tape->first;
                chosen_tape->first = chosen_record->next[index];
                if (output_tape->count == 0)
                    output_tape->first = chosen_record;
                else
                    output_tape->last->next[index] = chosen_record;
                output_tape->last = chosen_record;
                output_tape->count++;
            }
        }
    }

    if (tape[base].count > 1L)
        tape[base].last->next[index] = NULL;
    if (pcount != NULL)
        *pcount = tape[base].count;
    return tape[base].first;
}

