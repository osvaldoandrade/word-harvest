/* 
 * File:   main.c
 * Author: osvaldoandrade
 *
 * Created on 8 de Março de 2012, 19:14
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <dirent.h>
#include <regex.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <ctype.h>

#define MAX_LINE 10000

#define OK                   0
#define INSUFFICIENT_MEMORY  1
#define FILE_CREATION_ERROR  2
#define FILE_WRITE_ERROR     3
#define point_ms ((struct read_write_compare_ms *) pointer)
#define buf_ms   ((struct numbered_record_ms *) buffer)

/**
 * @struct read_write_compare_ms
 * @brief Structure representing a data structure for reading, writing, and comparing data from files.
 *
 * This structure contains references to two file pointers, functions for reading, writing, and comparing data,
 * a pointer to any additional data needed, and a record number to keep track of the current record being processed.
 */
struct read_write_compare_ms {
    FILE *unsorted_file;
    FILE *sorted_file;

    int (*read)(FILE *, void *, void *);

    int (*write)(FILE *, void *, void *);

    int (*compare)(void *, void *, void *);

    void *pointer;
    unsigned long record_number;
};

/**
 * @class numbered_record_ms
 * @brief Represents a numbered record with a variable sized text record.
 *
 * The numbered_record_ms struct represents a numbered record with a variable sized text record. The text record is stored
 * as a null-terminated string. The struct contains two fields: number (unsigned long) and record (char array).
 */
struct numbered_record_ms {
    unsigned long number;
    char record[1];
};

/**
 * @brief A helper class for managing a linked list of records stored in memory.
 */
struct record_in_memory_helper {
    struct record_in_memory_helper *next;
    char record[1];
};

/**
 * @struct compare_info_helper
 * @brief Helper struct for performing comparisons
 *
 * This struct encapsulates the necessary information for performing
 * comparisons between two objects.
 */
struct compare_info_helper {
    int (*compare)(void *, void *, void *);

    void *pointer;
};

static void
scan_dir(char *input_dir, FILE *output_file);

static int
parse_file(char *path_name, FILE *output_file);

static int
configure(char **input_dir, char **output_file, char **ext, int argc, char **argv);

static void
print_usage();

static int
extract_file_ext_regex(char **destin, const char *ext);

static void
release_resources();

static char *
trim(char *str);

static int
write_record(FILE *f, void *buffer, void *pointer);

static int
read_record(FILE *f, void *buffer, void *pointer);

static int
compare(void *p, void *q, void *pointer);

static int
compare_case(void *p, void *q, void *pointer);

static int
remove_same_words(FILE *input, FILE *ouput);

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

void *sort_linked_list(void *head, unsigned index, int (*compare)(void *, void *, void *), void *pointer,
                       unsigned long *pcount);

static char *g_file_ext_regex;
static FILE *g_output_file;
static FILE *g_unsorted_file;
static regex_t g_ext_regex;
static regex_t g_word_regex;


int main(int argc, char **argv) {
    char *input_dir, *output_file, *ext;

    // Define error messages for common errors
    static char *error_messages[] = {
            "INSUFFICIENT MEMORY",
            "FILE CREATION ERROR",
            "FILE WRITE ERROR"
    };

    // Configure input directory, output file, and extension based on command-line arguments
    if (configure(&input_dir, &output_file, &ext, argc, argv) != 0) {
        return EXIT_FAILURE; // Exit if configuration fails
    }

    FILE *unsorted_file = tmpfile();
    if (unsorted_file == NULL) {
        perror("Error creating tmp file for scanning directory");
        exit(EXIT_FAILURE);
    }

    // Perform a recursive scan of the input directory and process files
    scan_dir(input_dir, unsorted_file);

    FILE *sorted_file = tmpfile();
    if (sorted_file == NULL) {
        perror("Error opening temporary file");
        exit(EXIT_FAILURE);
    }

    rewind(unsorted_file);

    // Sort the contents of the unsorted file and write to the sorted temporary file
    int error_code = stable_merge_sort(unsorted_file, sorted_file, read_record, write_record,
                                       compare, NULL, MAX_LINE + 1, 150000 / (MAX_LINE + 5), NULL);
    if (error_code != 0) {
        // Print sorting error message and return failure
        fprintf(stderr, "SORT: %s\n", error_messages[error_code - 1]);
        return EXIT_FAILURE;
    }

    // Create another temporary file for storing results after removing duplicate words
    FILE *clean_sorted_file = tmpfile();
    if (clean_sorted_file == NULL) {
        perror("Error opening temporary file"); // Print error and exit if file creation fails
        exit(EXIT_FAILURE);
    }

    // Reset the pointer of the sorted file to the beginning
    fseek(sorted_file, 0, SEEK_SET);

    // Remove duplicate words from the sorted file and write to the clean sorted file
    error_code = remove_same_words(sorted_file, clean_sorted_file);
    if (error_code != 0) {
        perror("Error removing duplicate words"); // Print error if removal fails
    }

    // Reset the pointer of the clean sorted file to the beginning
    fseek(clean_sorted_file, 0, SEEK_SET);

    // Perform a final sorting of the clean sorted file and write to the global output file
    error_code = stable_merge_sort(clean_sorted_file,
                                   g_output_file,
                                   read_record,
                                   write_record,
                                   compare_case,
                                   NULL,
                                   MAX_LINE + 1,
                                   150000 / (MAX_LINE + 5),
                                   NULL);
    if (error_code != 0) {
        // Print sorting error message and return failure
        fprintf(stderr, "SORT: %s\n", error_messages[error_code - 1]);
        return EXIT_FAILURE;
    }

    // Close all opened files to release resources
    fclose(clean_sorted_file);
    fclose(g_unsorted_file);
    fclose(sorted_file);
    fclose(g_output_file);

    // Release any other allocated resources
    release_resources();

    // Exit the program successfully
    return EXIT_SUCCESS;
}

/**
 * \brief Configures the program based on the command line arguments.
 *
 * This function parses the command line arguments and sets the respective variables.
 * If the arguments are not provided correctly, it prints the usage and exits.
 *
 * \param input_dir [out] The absolute path of the input directory.
 * \param output_file [out] The output file path.
 * \param ext [out] The file extensions to be filtered.
 * \param argc The number of command line arguments.
 * \param argv The command line arguments array.
 *
 * \return 0 on success, -1 if there is an error.
 */
static int
configure(char **input_dir, char **output_file, char **ext, int argc, char **argv) {
    int err = 0;
    if (argc >= 5) {

        for (int i = 1; i < argc; i++) {
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

                if ((g_output_file = fopen(*output_file, "ab+")) == NULL) {
                    perror("Erro ao abrir arquivo de saida");
                    exit(1);
                }

                if ((g_unsorted_file = tmpfile()) == NULL) {
                    perror("Erro ao abrir arquivo de saida");
                    exit(1);
                }
            }

            if (strcmp(argv[i], "-e") == 0) {
                if (argv[++i] == NULL) {
                    print_usage();
                    exit(1);
                }
                *ext = malloc(strlen(argv[i]));
                strncpy(*ext, argv[i], strlen(argv[i]));

                err = extract_file_ext_regex(&g_file_ext_regex, *ext);
            }
        }

        if (g_file_ext_regex == NULL) {
            *ext = malloc(8);
            strncpy(*ext, "txt:text", 8);
            err = extract_file_ext_regex(&g_file_ext_regex, *ext);
            printf("Filtro utilizado: %s", g_file_ext_regex);

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


typedef struct QueueNode {
    char *path;
    struct QueueNode *next;
} QueueNode;

typedef struct {
    QueueNode *front;
    QueueNode *rear;
} Queue;

void enqueue(Queue *q, const char *path) {
    QueueNode *newNode = malloc(sizeof(QueueNode));
    newNode->path = strdup(path);
    newNode->next = NULL;
    if (q->rear == NULL) {
        q->front = q->rear = newNode;
        return;
    }
    q->rear->next = newNode;
    q->rear = newNode;
}

char *dequeue(Queue *q) {
    if (q->front == NULL) return NULL;
    QueueNode *temp = q->front;
    char *path = temp->path;
    q->front = q->front->next;
    if (q->front == NULL) q->rear = NULL;
    free(temp);
    return path;
}

int isEmpty(Queue *q) {
    return q->front == NULL;
}

void freeQueue(Queue *q) {
    while (!isEmpty(q)) {
        free(dequeue(q));
    }
}

/**
 * @brief Scans a directory recursively and parses files matching a specified file extension.
 *
 * This function scans a directory and its subdirectories recursively. It parses files that have a matching file extension
 * using a regular expression. The parsed content is then written to an output file.
 *
 * @param[in] input_dir The path of the input directory to scan.
 * @param[in] output_file The path of the output file to write the parsed content.
 */
void scan_dir(char *input_dir, FILE *output_file) {

    char err_msg[200];
    int err;

    // Compila a expressão regular para a extensão do arquivo
    if ((err = regcomp(&g_ext_regex, g_file_ext_regex, REG_EXTENDED)) != 0) {
        regerror(err, &g_ext_regex, err_msg, sizeof(err_msg));
        printf("Erro ao compilar expressão regular '%s': %s.\n", g_file_ext_regex, err_msg);
        return; // Sai da função se a compilação da regex falhar
    }

    Queue dirs;
    dirs.front = dirs.rear = NULL;
    enqueue(&dirs, input_dir);

    char *current_dir;

    while ((current_dir = dequeue(&dirs)) != NULL) {
        DIR *dirPtr = opendir(current_dir);
        if (dirPtr == NULL) {
            perror("Unable to open directory");
            free(current_dir);
            continue;
        }

        struct dirent *dp;
        while ((dp = readdir(dirPtr)) != NULL) {
            if (strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0) continue;

            size_t path_len = strlen(current_dir) + strlen(dp->d_name) + 2;
            char *path_name = malloc(path_len);
            snprintf(path_name, path_len, "%s/%s", current_dir, dp->d_name);

            struct stat buf;
            if (stat(path_name, &buf) == 0) {
                if (S_ISDIR(buf.st_mode)) {
                    enqueue(&dirs, path_name);
                } else if (S_ISREG(buf.st_mode) && regexec(&g_ext_regex, dp->d_name, 0, NULL, 0) == 0) {
                    parse_file(path_name, output_file);
                }
            }
            free(path_name);
        }
        closedir(dirPtr);
        free(current_dir);
    }

    freeQueue(&dirs);
    regfree(&g_ext_regex);
}

/**
 * @brief Parses a file, extracting words and writing them to an output file.
 *
 * This function reads a file, extracts individual words, trims them, and writes them to an output file.
 *
 * @param path_name The path of the file to parse.
 * @param output_file The file to write the extracted words to. Must be opened with write permissions.
 * @return 0 on success, -1 on failure.
 */
static int
parse_file(char *path_name, FILE *output_file) {
    int fd;
    ssize_t n;
    char buf_tmp[4096];
    int offset, err;
    regex_t word_regex;

    // Compile the regex for word matching
    if ((err = regcomp(&word_regex, "[a-zA-Z0-9]+", REG_EXTENDED)) != 0) {
        return -1; // Retorna imediatamente em caso de falha ao compilar a regex
    }

    fd = open(path_name, O_RDONLY);
    if (fd == -1) {
        perror("Erro ao abrir arquivo");
        regfree(&word_regex); // Libera a regex compilada
        return -1;
    }

    while ((n = read(fd, buf_tmp, sizeof(buf_tmp))) > 0) {
        offset = 0;
        regmatch_t rm;
        int flags = 0; // Nenhuma flag especial para a primeira execução
        while (regexec(&word_regex, buf_tmp + offset, 1, &rm, flags) == 0) {
            int len = rm.rm_eo - rm.rm_so;
            char *word = malloc(len + 1);
            if (word == NULL) {
                perror("Falha na alocação de memória");
                break;
            }
            memcpy(word, buf_tmp + offset + rm.rm_so, len);
            word[len] = '\0';

            char *trimmed_word = trim(word);
            fprintf(output_file, "%s\n", trimmed_word); // Usa diretamente o ponteiro de arquivo passado

            free(word);
            offset += rm.rm_eo;
            if (offset >= n) break; // Verifica se alcançou o final do buffer lido
            flags = REG_NOTBOL;
        }
    }

    if (n < 0) perror("Erro ao ler arquivo");

    close(fd);
    regfree(&word_regex); // Libera a regex compilada

    return 0; // Retorna sucesso
}

/**
 * @brief Release the resources used by the regular expressions.
 *
 * This function releases the resources used by the `g_word_regex` and `g_ext_regex`
 * regular expressions. It should be called when you are finished using the regular
 * expressions to free any allocated memory and cleanup resources.
 *
 * @note Make sure that you have finished using the regular expressions before calling
 * this function, as any further use of the regular expressions after calling this
 * function may result in undefined behavior.
 */
static void
release_resources() {
    regfree(&g_word_regex);
    regfree(&g_ext_regex);
}


/**
 * @brief Extracts file extensions from a string and constructs a regular expression pattern.
 *
 * This function takes a string containing file extensions separated by colons (:)
 * and constructs a regular expression pattern in the format: (\.ext1)|(\.ext2)|...|(\.extN)
 * The resulting pattern is stored in the destination string.
 *
 * @param[in,out] destin The destination string to store the constructed pattern.
 * @param[in] ext The input string containing file extensions separated by colons.
 *
 * @return 0 on success, -1 on error.
 */
static int
extract_file_ext_regex(char **destin, const char *ext) {
    if (!ext || !destin) {
        return -1;
    }

    char *tokens = strdup(ext);
    char *result = strtok(tokens, ":");
    if (!result) {
        free(tokens);
        return -1;
    }

    *destin = malloc(strlen(result) + 4);
    if (!(*destin)) {
        free(tokens);
        return -1;
    }
    strncpy(*destin, "(\\.", 4);
    strncat(*destin, result, strlen(result));
    strncat(*destin, ")", 2);
    while ((result = strtok(NULL, ":")) != NULL) {
        char *tmp = malloc(strlen(result) + 5);
        if (!tmp) {
            free(*destin);
            *destin = NULL;
            free(tokens);
            return -1;
        }
        strncpy(tmp, "|(\\.", 5);
        strncat(tmp, result, strlen(result));
        strncat(tmp, ")", 2);
        *destin = realloc(*destin, strlen(*destin) + strlen(tmp) + 1);
        if (!(*destin)) {
            free(tmp);
            free(tokens);
            return -1;
        }
        strncat(*destin, tmp, strlen(tmp));
        free(tmp);
    }
    free(tokens);
    return 0;
}


/**
 * @brief Prints the usage instructions for the program.
 *
 * This function prints the list of parameters and an example usage of the program.
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

char *
trim(char *str) {
    char *end;

    // Trim leading space
    while (isspace((unsigned char) *str)) str++;

    if (*str == 0)  // All spaces?
        return str;

    // Trim trailing space
    end = str + strlen(str) - 1;
    while (end > str && isspace((unsigned char) *end)) end--;

    // Write new null terminator character
    end[1] = '\0';

    return str;
}

static int compare(void *p, void *q, void *pointer) {

    const char *pp = (const char *) p;
    const char *qq = (const char *) q;

    return strcmp(pp, qq);
}

static int compare_case(void *p, void *q, void *pointer) {

    return strcasecmp((const char *) p, (const char *) q);
}

/**
 * @brief Reads a record from a file.
 *
 * This function reads a record from a file. It reads characters from the file until it reaches
 * a newline character or the end of file. The characters are stored in the provided buffer. If the
 * length of the record exceeds the maximum line length, an error message is printed to stderr
 * and the program exits.
 *
 * @param f The file pointer from which to read the record.
 * @param buffer A pointer to the buffer to store the record.
 * @param pointer Unused parameter.
 * @return The number of characters read, excluding the null terminator.
 */
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

/**
 * @brief Write a record to a file.
 *
 * @param f         Pointer to the file where the record will be written.
 * @param buffer    Pointer to the buffer containing the record to be written.
 * @param pointer   Pointer to any additional data if required by the implementation.
 *
 * @return int      Returns 1 if the record is successfully written to the file, 0 otherwise.
 *
 * @note The `buffer` parameter should be a null-terminated string.
 *
 * This function writes a record to the specified file. It expects the `buffer` parameter to be a pointer
 * to a null-terminated string representing the record. The `f` parameter should be a pointer to an open file
 * where the record will be written. The `pointer` parameter is reserved for any additional data that may be
 * required by the implementation.
 *
 * The function returns 1 if the record is successfully written to the file, and 0 if there was an error
 * during the writing process.
 */
static int
write_record(FILE *f, void *buffer, void *pointer) {
    return fputs((char *) buffer, f) != EOF && putc('\n', f) != EOF;
}

/**
 * @brief Remove duplicate words from an input file and write the result to an output file.
 *
 * This function reads each line from the input file and removes duplicate consecutive words. The resulting
 * lines are written to the output file.
 *
 * @param input The input file to read from.
 * @param output The output file to write the result to.
 *
 * @return 0 if the operation is successful, otherwise 1.
 */
static int
remove_same_words(FILE *input, FILE *ouput) {
    ssize_t read_bytes;
    size_t offset = 0;
    char *current_word = NULL;
    char *last_word = NULL;

    fseek(input, 0L, SEEK_SET);
    while ((read_bytes = getline(&current_word, &offset, input)) > 0) {
        if (last_word == NULL || strcmp(current_word, last_word) != 0) {
            if (fwrite(current_word, strlen(current_word), 1, ouput) < 1) {
                free(current_word);
                free(last_word);
                return 1;
            }
        }
        free(last_word);
        last_word = strndup(current_word, strlen(current_word));
    }
    free(current_word);
    free(last_word);
    return 0;
}

/**
 * @brief Reads a record from a file, updates the record number, and returns the number of bytes read.
 *
 * This function reads a record from a file. If the file is the unsorted file specified in the point_ms struct,
 * the record number is updated using the record_number field in the struct. Otherwise, the record number is
 * read from the file and stored in the number field of the buf_ms struct. The function calls the read function
 * specified in the point_ms struct to read the record data.
 *
 * @param f Pointer to the file to read from.
 * @param buffer Pointer to the buffer to store the record data.
 * @param pointer Pointer to a data structure to be passed to the read function.
 *
 * @return The number of bytes read from the file, including the size of the record number.
 *         If an error occurs, returns a non-zero value.
 */
static int
read_record_ms(FILE *f, void *buffer, void *pointer) {
    int n;
    if (f == point_ms->unsorted_file)
        buf_ms->number = point_ms->record_number++;
    else
        fread(&buf_ms->number, sizeof(unsigned long), 1, f);
    n = (*point_ms->read)(f, buf_ms->record, point_ms->pointer);
    if (n != 0)
        n += sizeof(unsigned long);
    return n;
}

/**
 * @brief Writes a record to a file using a specified write function.
 *
 * This function writes a record to a file using the provided write function.
 * If the file is not the sorted file associated with the pointer, the record
 * number is also written to the file. The function returns 0 if the write
 * operation fails, otherwise it returns a non-zero value.
 *
 * @param f         The file to write the record to.
 * @param buffer    A pointer to the record to write.
 * @param pointer   A pointer to the related data structure.
 *
 * @return 0 if the write operation fails, otherwise a non-zero value.
 */
static int
write_record_ms(FILE *f, void *buffer, void *pointer) {
    if (f != point_ms->sorted_file &&
        fwrite(&buf_ms->number, sizeof(unsigned long), 1, f) == 0) {
        return 0;
    }
    return (*point_ms->write)(f, buf_ms->record, point_ms->pointer);
}

/**
 * @brief Compare function for sorting records based on a specified comparison function and record number.
 *
 * This function is used to compare two records in order to sort them. It takes in two void pointers `p` and `q`,
 * which are cast to `struct numbered_record_ms` pointers `pp_ms` and `qq_ms` respectively.
 *
 * The comparison is done by calling the `compare` function provided in the `struct read_write_compare_ms` pointed
 * to by the `pointer` argument. If the comparison function returns 0, then the records' numbers are compared
 * to determine the final order.
 *
 * @param p The first record to compare.
 * @param q The second record to compare.
 * @param pointer A pointer to the `struct read_write_compare_ms` struct containing comparison and pointer functions.
 * @return 0 if the records are equal, a negative value if the first record is less than the second, and a positive
 *         value if the first record is greater than the second.
 */
static int
compare_records_ms(void *p, void *q, void *pointer) {
#define pp_ms ((struct numbered_record_ms *) p)
#define qq_ms ((struct numbered_record_ms *) q)
    int n = (*point_ms->compare)(pp_ms->record, qq_ms->record, point_ms->pointer);
    if (n == 0)
        n = pp_ms->number < qq_ms->number ? -1 : 1;
    return n;
}

/**
 * @brief Stable merge sort function that sorts a file using an external sort algorithm.
 *
 * This function takes an unsorted file and sorts it using the merge sort algorithm. The sorted
 * file is written to the specified output file. The function uses the provided read, write, and
 * compare functions to read, write, and compare records. The pointer parameter is passed to these
 * functions as an additional argument. The max_record_size parameter specifies the maximum size
 * of a record in the file. The block_size parameter specifies the number of records to be sorted
 * at once in memory. The record_count parameter, if not NULL, will store the number of records in
 * the sorted file.
 *
 * @param unsorted_file The unsorted input file.
 * @param sorted_file The output file to write the sorted records.
 * @param read The function to read a record from the file.
 * @param write The function to write a record to the file.
 * @param compare The function to compare two records.
 * @param pointer An additional parameter to be passed to the read, write, and compare functions.
 * @param max_record_size The maximum size of a record in the file.
 * @param block_size The number of records to be sorted at once in memory.
 * @param record_count Pointer to store the number of records in the sorted file. Can be NULL.
 * @return Returns OK if the sort operation was successful. Returns an error code otherwise.
 *
 * @see struct read_write_compare_ms
 * @see merge_sort
 */
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

    return merge_sort(unsorted_file,
                      sorted_file,
                      read_record_ms,
                      write_record_ms,
                      compare_records_ms,
                      &rwc,
                      sizeof(unsigned long) + max_record_size,
                      block_size,
                      record_count);
}

/**
 * @brief Helper function to free memory blocks in a linked list.
 *
 * This function iterates through a linked list of memory blocks and frees each block.
 *
 * @param first The first memory block in the linked list.
 */
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

/**
 * @brief Sorts the records in an unsorted file and writes them to a sorted file using merge sort algorithm.
 *
 * The function reads blocks of records from the unsorted file, sorts them in memory, and writes them alternately to two temporary tapes.
 * It performs merging of the temporary tapes until every record is in the first tape.
 * The final sorted records are then written to the specified sorted file.
 *
 * @param unsorted_file The unsorted file containing the records to be sorted.
 * @param sorted_file The file where the sorted records will be written.
 * @param read A function pointer to the function responsible for reading a record from the file.
 * @param write A function pointer to the function responsible for writing a record to the file.
 * @param compare A function pointer to the function used for comparing two records.
 * @param pointer A pointer to additional data that may be required by the read, write, and compare functions.
 * @param max_record_size The maximum size of a record.
 * @param block_size The size of a block to be read from the file.
 * @param pcount A pointer to a variable where the total number of sorted records will be stored. Set to NULL if not needed.
 *
 * @return Returns one of the following codes:
 * - INSUFFICIENT_MEMORY: Unable to allocate memory.
 * - FILE_CREATION_ERROR: Error occurred while creating a temporary file.
 * - FILE_WRITE_ERROR: Error occurred while writing to a file.
 * - OK: Sorting completed successfully.
 */
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
                        malloc(sizeof(struct record_in_memory_helper *) + record_size);
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
            memcpy(source_tape, destination_tape, sizeof(source_tape));
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

/**
 * @brief Sorts a linked list using a modified merge sort algorithm.
 *
 * This function sorts a linked list in ascending order using a modified merge sort algorithm.
 * The function expects a pointer to the head of the linked list, the index of the next pointer in the record structure,
 * a comparison function that compares two records, a pointer to any additional data needed for the comparison function,
 * and a pointer to a variable to store the number of records in the sorted list.
 *
 * The linked list is sorted in-place, and the function returns a pointer to the head of the sorted list.
 *
 * @param head The pointer to the head of the linked list.
 * @param index The index of the next pointer in the record structure.
 * @param compare The comparison function that compares two records.
 * @param pointer A pointer to any additional data needed for the comparison function.
 * @param pcount A pointer to a variable to store the number of records in the sorted list.
 * @return A pointer to the head of the sorted list.
 */
void *
sort_linked_list(void *head, unsigned index, int (*compare)(void *, void *, void *), void *pointer,
                 unsigned long *pcount) {

    typedef struct record {
        struct record *next[1]; // Pointer to the next element in the list
    } record;

    typedef struct tape {
        record *head, *tail; // Head and tail pointers for the tape
        unsigned long count; // Number of records on this tape
    } tape;

    unsigned base; // Base index for the tapes
    unsigned long block_size; // Current size of sorted blocks

    tape tapes[4]; // Array of tapes for distribution and collection phases

    // Initialize tapes
    tapes[0].count = tapes[1].count = 0L;
    tapes[0].head = NULL;
    base = 0;

    // Distribute the records alternately between tapes[0] and tapes[1]
    while (head != NULL) {
        record *next_record = ((record *) head)->next[index];
        ((record *) head)->next[index] = tapes[base].head;
        tapes[base].head = (record *) head;
        tapes[base].count++;
        head = next_record;
        base ^= 1; // Alternate between 0 and 1
    }

    // Merge phase: Merge blocks from tapes[0] and tapes[1] into tapes[2] and tapes[3], and so on
    for (base = 0, block_size = 1L; tapes[base + 1].count != 0L; base ^= 2, block_size <<= 1) {
        int dest_tape_index;
        tape *src_tape0, *src_tape1;
        src_tape0 = &tapes[base];
        src_tape1 = &tapes[base + 1];
        dest_tape_index = base ^ 2;
        tapes[dest_tape_index].count = tapes[dest_tape_index + 1].count = 0;
        for (; src_tape0->count != 0; dest_tape_index ^= 1) {
            unsigned long n0, n1;
            tape *dest_tape = &tapes[dest_tape_index];
            n0 = n1 = block_size;
            while (1) {
                record *selected_record;
                tape *selected_tape;
                if (n0 == 0 || src_tape0->count == 0) {
                    if (n1 == 0 || src_tape1->count == 0) break;
                    selected_tape = src_tape1;
                    n1--;
                } else if (n1 == 0 || src_tape1->count == 0) {
                    selected_tape = src_tape0;
                    n0--;
                } else if ((*compare)(src_tape0->head, src_tape1->head, pointer) > 0) {
                    selected_tape = src_tape1;
                    n1--;
                } else {
                    selected_tape = src_tape0;
                    n0--;
                }
                // Move the selected record to the destination tape
                selected_tape->count--;
                selected_record = selected_tape->head;
                selected_tape->head = selected_record->next[index];
                if (dest_tape->count == 0) dest_tape->head = selected_record;
                else dest_tape->tail->next[index] = selected_record;
                dest_tape->tail = selected_record;
                dest_tape->count++;
            }
        }
    }

    // Finalize the list on the last tape used
    if (tapes[base].count > 1L) tapes[base].tail->next[index] = NULL;
    if (pcount != NULL) *pcount = tapes[base].count;
    return tapes[base].head;
}
