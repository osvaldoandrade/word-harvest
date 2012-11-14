
/* Stable Merge Sort
   by Philip J. Erdelsky
   pje@efgh.com
   http://www.alumni.caltech.edu/~pje/
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern int merge_sort(FILE *, FILE *,
  int (*)(FILE *, void *, void *),
  int (*)(FILE *, void *, void *),
  int (*)(void *, void *, void *), void *,
  unsigned, unsigned long, unsigned long *);

struct read_write_compare_ms
{
  FILE *unsorted_file;
  FILE *sorted_file;
  int (*read)(FILE *, void *, void *);
  int (*write)(FILE *, void *, void *);
  int (*compare)(void *, void *, void *);
  void *pointer;
  unsigned long record_number;
};

struct numbered_record_ms
{
  unsigned long number;
  char record[1];
};

#define point_ms ((struct read_write_compare_ms *) pointer)
#define buf_ms   ((struct numbered_record_ms *) buffer)

static int read_record_ms(FILE *f, void *buffer, void *pointer)
{
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

static int write_record_ms(FILE *f, void *buffer, void *pointer)
{
  if (f != point_ms->sorted_file &&
    fwrite(&buf_ms->number, sizeof(unsigned long), 1, f) == 0)
  {
    return 0;
  }
  return (*point_ms->write)(f, buf_ms->record, point_ms->pointer);
}

static int compare_records_ms(void *p, void *q, void *pointer)
{
  #define pp ((struct numbered_record_ms *) p)
  #define qq ((struct numbered_record_ms *) q)
  int n = (*point_ms->compare)(pp->record, qq->record, point_ms->pointer);
  if (n == 0)
    n = pp->number < qq->number ? -1 : 1;
  return n;
}

int stable_merge_sort(FILE *unsorted_file, FILE *sorted_file,
  int (*read)(FILE *, void *, void *),
  int (*write)(FILE *, void *, void *),
  int (*compare)(void *, void *, void *), void *pointer,
  unsigned max_record_size, unsigned long block_size,
  unsigned long *record_count)
{
  struct read_write_compare_ms rwc;
  rwc.unsorted_file = unsorted_file;
  rwc.sorted_file = sorted_file;
  rwc.read = read;
  rwc.write = write;
  rwc.compare = compare;
  rwc.pointer = pointer;
  rwc.record_number = 0L;
  return merge_sort(unsorted_file, sorted_file, read_record_ms, write_record_ms,
    compare_records_ms, &rwc, sizeof(unsigned long) + max_record_size,
    block_size, record_count);
}
