/**********************************************************
 * Pager for assignments in the Databases course INF-2700 *
 * UIT - The Arctic University of Norway                  *
 * Author: Weihai Yu                                      *
 **********************************************************/

#include "pager.h"
#include "pmsg.h"
#include <unistd.h>
#include <sys/stat.h>
#include <string.h>
#include <fcntl.h>

/** the dir in which the database files are stored */
char sys_dir[512];

/** @brief Database file handle */
typedef struct file_handle_struct {
  char *fname;  /**< file name */
  int fd;       /**< Unix file descriptor */
  /** The blocks (max NUM_PAGES) currently in the memory.
      blocks_in_mem[i] == NULL when that slot is left unused.
  */
  int num_blocks; /**< number of blocks this file has. */
  block_p blocks_in_mem[NUM_PAGES];
  block_p current_block; /**current block been accessd */
} file_handle_struct;

typedef struct file_handle_struct * fhandle_p;

/** Handles of all files that are open */
fhandle_p file_handles[MAX_OPEN_FILES];

typedef struct pq_elm * pq_elm_p;

/** @brief Database file block */
typedef struct block_struct {
  fhandle_p fhandle; /**< file handle */
  int blk_nr;            /**< block number */
  page_p page;           /**< buffer page of the block */
} block_struct;

/** @brief Database buffer page

The content of a page/block consists of first a header,
then a series of records, and finally free space.

The header includes:
 - bytes 0-3: header size
 - bytes 4-7: position of the beginning of the unused space
 - possibly some more, for example, when implementing variable-length records,
   file as linked list of blocks, or lsn for write-ahead logging
*/

typedef struct page_struct {
  char *content;   /**< BLOCK_SIZE of bytes */
  int page_nr;
  block_p block;   /**< the correspoding file block */
  pq_elm_p qelm;   /**< the corresponding elm in pfifo */
  int pinned;      /**< non-zoro if the block is pinned to the page */
  int dirty;       /**< non-zero if the content has been changed (dirty) */
  int free_pos;    /**< beginning of free space */
  int current_pos; /**< current position for next access */
} page_struct;

/** page queue */

/** @brief element in pqueue */
typedef struct pq_elm {
  page_p page;   /**< pointing to the queued page */
  pq_elm_p prev; /**< previous page */
  pq_elm_p next; /**< next page */
} pq_elm;

/** @brief queue of pages */
typedef struct pqueue {
  pq_elm_p first;
  pq_elm_p last;
  int len;
} pqueue;

typedef pqueue * pqueue_p;

/** Page LRF queues for page replacement */
static pqueue_p q_pinned, q_unpinned;

/** Pager profiler */
static struct {
  int num_seeks;       /**< number of seeks after the reset of pager profiler */
  int num_disk_reads;  /**< number of disk reads after the reset of pager profiler */
  int num_disk_writes; /**< number of disk writes after the reset of pager profiler */
  int last_fd;     /** fd of the last visited block, used to check if a new seek is needed */
  int last_blk_nr; /** nr of the last visited block, used to check if a new seek is needed */
} pager_profiler;


/** The number of files that are currently open */
int num_file_handles = 0;

page_p pages[NUM_PAGES];

static void put_fhandle_info(pmsg_level level, fhandle_p fh) {
  if (!fh) {
    put_msg(level, "NULL file handle\n");
    return;
  }
  put_msg(level, "  fname: \"%s\", fd: %d, ", fh->fname, fh->fd);
  append_msg(level, "%d blocks, current: %d.\n",
             fh->num_blocks,
             fh->current_block ? fh->current_block->blk_nr : -100);
  put_msg(level, "   in memory: ");
  for (size_t i = 0; i < NUM_PAGES; i++)
    if (fh->blocks_in_mem[i])
      append_msg(level,  " %d,", fh->blocks_in_mem[i]->blk_nr);
  append_msg(level,  "\n");
}

/* forward declaration */
static fhandle_p get_tbl_file(char const* fname);

void put_file_info(pmsg_level level, char const* name) {
  fhandle_p fh = get_tbl_file(name);
  if (!fh)
    put_msg(level, "file \"%s\" not open.\n", name);
  else
    put_fhandle_info(level, fh);
}

void put_page_info(pmsg_level level, page_p p) {
  if (!p) {
    put_msg(level, "  NULL page\n");
    return;
  }
  if (!p-> block) {
    put_msg(level,  "  unused\n");
    return;
  }
  put_msg(level, "  current_pos: %d, ", p->current_pos);
  append_msg(level, "  free_pos: %d, ", p->free_pos);

  if (p->pinned == 0)
    append_msg(level,  "unpinned, ");
  else
    append_msg(level,  "pinned, ");

  if (p->dirty == 0)
    append_msg(level,  "clean\n");
  else
    append_msg(level,  "dirty\n");

  put_block_info(level, p->block);
}

void put_block_info(pmsg_level level, block_p b) {
  if (!b) return;
  put_msg(level,  "    block: ");
  if (b-> fhandle)
    append_msg(level, "file: %s, ", b->fhandle->fname);
  append_msg(level,  "blk_nr: %d, page_nr: %d\n",
             b->blk_nr, b->page->page_nr);
}

void put_pager_info(pmsg_level level,  char const* msg) {
  if (!msg) msg = "";
  put_msg(level,  "----Pager Info Begin----\n");
  put_msg(level,  "(%s)\n", msg);
  put_msg(level, "file handlers:\n");
  for (size_t i = 0; i < MAX_OPEN_FILES; i++)
    if (file_handles[i]) {
      put_msg(level,  " %d:\n", i);
      put_fhandle_info(level, file_handles[i]);
    }

  put_msg(level, "pages:\n");
  for (size_t i = 0; i < NUM_PAGES; i++)
    if (pages[i]) {
      put_msg(level,  " page  %d:\n", i);
      put_page_info(level, pages[i]);
    }

  put_msg(level,  "----Pager Info End ----\n");
}

void put_pager_profiler_info(pmsg_level level) {
  put_msg(level, "Number of disk seeks/reads/writes/IOs: %d/%d/%d/%d\n",
          pager_profiler.num_seeks,
          pager_profiler.num_disk_reads,
          pager_profiler.num_disk_writes,
          pager_profiler.num_disk_reads + pager_profiler.num_disk_writes);
}

static void put_pqueue_info(pmsg_level level, pqueue_p q,
                            char const* which) {
  if (!q) {
    put_msg(level, "Page LRF %s is NULL\n", which);
    return;
  }
  put_msg(level, "Page LRF %s, length %d:\n", which, q->len);
  pq_elm_p p = q->first;
  while (p) {
    append_msg(level, "  %d,", p->page->page_nr);
    if (p == q->last) {
      p = 0;
      break;
    }
    p = p->next;
  }
  append_msg(level, "\n");
}

void put_pqueues_info(pmsg_level level) {
  put_pqueue_info(level, q_unpinned, "unpinned");
  put_pqueue_info(level, q_pinned, "pinned");
}


void pager_profiler_reset(void) {
  pager_profiler.num_seeks = 0;
  pager_profiler.num_disk_reads = 0;
  pager_profiler.num_disk_writes = 0;
  pager_profiler.last_fd = -1;
  pager_profiler.last_blk_nr = -1;
}

int set_system_dir(char const* dir) {
  if (!dir) return 0;

  if (sys_dir[0] != '\0') {
    put_msg(ERROR, "Cannot set system dir twice.\n", dir);
    return 0;
  }

  pager_terminate();
  if (chdir(dir) == -1) {
    if ((mkdir(dir, 0755) == -1) || (chdir(dir) == -1)) {
      put_msg(ERROR, "%s - Invalid dir for database.\n", dir);
      return 0;
    }
  }
  getcwd(sys_dir, sizeof sys_dir);
  put_msg(DEBUG, "db dir : %s\n", sys_dir);

  return pager_init();
}

char* system_dir() {
  return sys_dir;
}

/** Increment num_seeks if needed
    update last_fd, last_blk_nr */
static void inc_num_seeks_maybe(int fd, int blk_nr) {
  /* put_msg (DEBUG, "seeks_maybe: fd %d, blk: %d\n", fd, blk_nr); */
  if (fd != pager_profiler.last_fd
      || abs(blk_nr - pager_profiler.last_blk_nr) > 1)
    pager_profiler.num_seeks++;
  pager_profiler.last_fd = fd;
  pager_profiler.last_blk_nr = blk_nr;
}

/** Increment num_disk_reads */
static void inc_num_reads(int fd, int blk_nr) {
  inc_num_seeks_maybe(fd, blk_nr);
  pager_profiler.num_disk_reads++;
}

/** increment num_disk_writes */
static void inc_num_writes(int fd, int blk_nr) {
  inc_num_seeks_maybe(fd, blk_nr);
  pager_profiler.num_disk_writes++;
}

static int get_header_int_at(page_p  p, int offset) {
  if (offset < 0 || offset >= PAGE_HEADER_SIZE -INT_SIZE) {
    put_msg(ERROR,
            "get_header_int_at: offset %d out of range (%ld,%ld)\n",
            offset, 0L, PAGE_HEADER_SIZE - INT_SIZE - 1);
    exit(EXIT_FAILURE);
  }
  int res = (int) *((int *)((p->content) + offset));
  return res;
}

static int put_header_int_at(page_p p, int offset, int val) {
  if (offset < 0 || offset >= PAGE_HEADER_SIZE - INT_SIZE) {
    put_msg(ERROR,
            "put_header_int_at: offset %d out of range (%ld,%ld)\n",
            offset, 0L, PAGE_HEADER_SIZE - INT_SIZE - 1);
    return 0;
  }
  memcpy(p->content + offset, (char *) &val, INT_SIZE);
  p->dirty = 1;
  return 1;
}

/* When putting and getting header values, make sure not to change
   the current position */
static void init_page_header_size(page_p p) {
  put_header_int_at(p, 0, PAGE_HEADER_SIZE);
}

static void check_page_header_size(page_p p) {
  int header_size = get_header_int_at(p, 0);
  if (header_size != PAGE_HEADER_SIZE) {
    put_msg(FATAL,
            "Header size of block is %d, which is incompatible with %d of current system.\n",
            header_size, PAGE_HEADER_SIZE);
    exit(EXIT_FAILURE);
  }
}

static void set_page_free_pos(page_p p, int pos) {
  p->free_pos = pos;
  put_header_int_at(p, 4, pos);
}

static void set_page_free_pos_from_content(page_p p) {
  p->free_pos = get_header_int_at(p, 4);
}

static void init_page(page_p p) {
  if (!p) return;
  memset(p->content, 0, BLOCK_SIZE);
  init_page_header_size(p);
  set_page_free_pos(p, PAGE_HEADER_SIZE);
  p->qelm = 0;
  p->block = 0;
  p->pinned = 0;
  p->dirty = 0;
  p->current_pos = PAGE_HEADER_SIZE;
}

static page_p make_page(int page_nr) {
  page_p p = malloc(sizeof (page_struct));
  if (!p) {
    put_msg(ERROR, "make_page failed");
    return 0;
  }
  p->content = malloc(BLOCK_SIZE);
  if (!p->content) {
    free(p);
    put_msg(ERROR, "make_page failed");
    return 0;
  }
  init_page(p);
  p->page_nr = page_nr;
  return p;
}

static pqueue_p make_pqueue() {
  pqueue_p pq = malloc(sizeof (pqueue));
  pq->first = 0;
  pq->last = 0;
  pq->len = 0;
  return pq;
}

static pqueue_p release_pqueue(pqueue_p q) {
  if (!q) return 0;
  pq_elm_p nextp = q->first;
  pq_elm_p p;
  while (nextp) {
    p = nextp; nextp = p->next;
    free(p);
    if (nextp == q->last) {
      if (nextp != q->first)
        free(nextp); /* beware: already freed if last == first */
      nextp = 0;
    }
  }
  free(q);
  q = 0;
  return 0;
}

static pq_elm_p make_pq_elm(page_p pg) {
  pq_elm_p p = malloc(sizeof (pq_elm));
  p->page = pg;
  pg->qelm = p;
  return p;
}

/* pg becomes the last in q */
static void pq_insert(pqueue_p q, pq_elm_p p) {
  if (!q->first) {
    p->prev = p;
    p->next = p;
    q->first = p;
    q->last = p;
  } else {
    p->prev = q->last;
    p->next = q->first;
    q->first->prev = p;
    q->last->next = p;
    q->last = p;
  }
  q->len++;
  return;
}

static void pq_remove(pqueue_p q, pq_elm_p p) {
  if (q->len <= 1) { /* at most one element in q */
    q->first = 0;
    q->last = 0;
    q->len = 0;
  } else {
    p->next->prev = p->prev;
    p->prev->next = p->next;
    if (p == q->first)
      q->first = p->next;
    if (p == q->last)
      q->last = p->prev;
    q->len--;
  }
  return;
}

static void pq_enqueue(pqueue_p q, page_p pg) {
  if (!q || !pg) {
    put_msg(ERROR, "pq_enqueue: NULL pqueue or page.\n");
    return;
  }
  return pq_insert(q, make_pq_elm(pg));
}

/* pg is moved to the last in the LRF queue */
static void pq_touch(page_p pg) {
  if (!pg) {
    put_msg(WARN, "touching NULL page.\n");
    return;
  }
  pq_elm_p p = pg->qelm;
  if (!p) {
    put_msg(WARN, "touching NULL qelm.\n");
    return;
  }

  pqueue_p q = pg->pinned ? q_pinned : q_unpinned;
  if (p == q->last) return;
  pq_remove(q, p);
  pq_insert(q, p);
  return;
}

/* remove the first in q */
static page_p pq_dequeue_unpinned() {
  if (!q_unpinned->first) return 0;

  pq_elm_p p = q_unpinned->first;
  page_p pg = p->page;
  pq_remove(q_unpinned, p);
  free(p);
  return pg;
}

static page_p pq_dequeue_pinned() {
  if (!q_pinned->first) return 0;

  pq_elm_p p = q_pinned->first;
  page_p pg = p->page;
  unpin(pg);
  pq_remove(q_unpinned, p);
  free(p);
  return pg;
}

/* pg truns into pinned, move it to another queue */
static void pq_turn_pinned(page_p pg) {
  if (pg->pinned) return;
  pq_elm_p p = pg->qelm;
  pq_remove(q_unpinned, p);
  pq_insert(q_pinned, p);
  return;
}

/* pg turns into unpinned, move it to another queue */
static void pq_turn_unpinned(page_p pg) {
  if (!pg->pinned) return;
  pq_elm_p p = pg->qelm;
  pq_remove(q_pinned, p);
  pq_insert(q_unpinned, p);
  return;
}

/* Search the global file_handles[] to see if the file
   is already open and returns its position in file_handles[].
   Returns -1 if the file is not open.
*/
static int find_fhandle_i(char const* fname) {
  for (size_t i = 0, num_compared = 0;
       num_compared < num_file_handles;
       i++) {
    if (file_handles[i]) {
      if (strcmp(file_handles[i]->fname, fname) == 0)
        return i;
      num_compared++;
    }
  }
  return -1;
}

/* Search the global file_handles[] for an empty slot,
   to be used to store a new file handle.
   Returns -1 if there is no empty slot (the number of
   open files has reached MAX_OPEN_FILES).
*/
static int get_empty_fhandle_i() {
  for (size_t i = 0; i < MAX_OPEN_FILES; i++)
    if (!file_handles[i]) return i;
  return -1;
}

static fhandle_p make_fhandle(char const* fname, int fd) {
  fhandle_p fh = malloc(sizeof (file_handle_struct));
  fh->fname = malloc(strlen(fname) + 1);
  strcpy(fh->fname, fname);
  fh->fd = fd;
  fh->num_blocks = lseek(fd, (off_t) 0, SEEK_END) / BLOCK_SIZE;
  fh->current_block = 0;
  for (size_t i = 0; i < NUM_PAGES; i++)
    fh->blocks_in_mem[i] = 0;

  return fh;
}

static fhandle_p get_tbl_file(char const* fname) {
  int file_i = find_fhandle_i(fname);

  if (file_i == -1) return 0;

  return file_handles[file_i];
}

static fhandle_p open_tbl_file(char const* fname) {
  if (num_file_handles == MAX_OPEN_FILES) {
    put_msg(WARN,
            "open Cannot file %s because the limit %d of open files has reached.",
            fname, MAX_OPEN_FILES);
    return 0;
  }

  int fd = open(fname, O_RDWR, 0);
  if (fd == -1) {
    /* if the file does not exist, create one */
    if ((fd = creat(fname, 0600)) == -1) {
      put_msg(WARN, "Failed to create file %s.", fname);
      return 0;
    }

    /* close and open the created file again for read and write */
    if (close(fd) == -1 || (fd = open(fname, O_RDWR, 0)) == -1)
      return 0;
  }

  int empty_i = get_empty_fhandle_i();
  if (empty_i == -1) return 0;

  fhandle_p fh = make_fhandle(fname, fd);

  file_handles[empty_i] = fh;
  num_file_handles++;

  return fh;
}

int file_num_blocks(char const* fname) {
  fhandle_p fh = get_tbl_file(fname);
  if (!fh) fh = open_tbl_file(fname);

  if (!fh) {
    put_msg(ERROR, "file_num_blocks: cannot get file \"fname\".\n");
    return -1;
  }
  return fh->num_blocks;
}

/* forward declaration */
static void release_block(block_p b);

static void close_tbl_file(fhandle_p fhandle) {
  if (!fhandle) return;
  for (size_t i = 0; i < NUM_PAGES; i++) {
    if (fhandle->blocks_in_mem[i])
      release_block(fhandle->blocks_in_mem[i]);
  }
  if (close(fhandle->fd) == 0) {
    int file_i = find_fhandle_i(fhandle->fname);
    free(fhandle->fname);
    free(fhandle);
    file_handles[file_i] = 0;
    num_file_handles--;
  }
}

int close_file(char const* fname) {
  int i = find_fhandle_i(fname);
  if (i != -1)
    close_tbl_file(file_handles[i]);
  return i;
}

int pager_init(void) {
  num_file_handles = 0;

  /* global vars file_handles[] are initialized with NULL.
     in case they are not, or pager_init is called again
     after pager_terminate, initialize them anyway */
  for (size_t i = 0; i < MAX_OPEN_FILES; i++)
    file_handles[i] = 0;
  for (size_t i = 0; i < NUM_PAGES; i++) {
    pages[i] = make_page(i);
    if (pages[i] == NULL) {
      put_msg(ERROR, "pager_init failed");
      pager_terminate();
      return 0;
    }
  }
  q_pinned = make_pqueue();
  q_unpinned = make_pqueue();
  pager_profiler_reset();
  return 1;
}

static block_p get_buffered_blk_in_fhandle(fhandle_p fh, int bnr) {
  for (size_t i = 0; i < NUM_PAGES; i++)
    if (fh->blocks_in_mem[i]
        && fh->blocks_in_mem[i]->blk_nr == bnr) {
      pq_touch(fh->blocks_in_mem[i]->page);
      return fh->blocks_in_mem[i];
    }

  return 0;
}

static int set_blk_in_fhandle(fhandle_p fh, block_p b) {
  for (size_t i = 0; i < NUM_PAGES; i++)
    if (!fh->blocks_in_mem[i]) {
      fh->blocks_in_mem[i] = b;
      return i;
    }

  put_msg(ERROR, "set_blk_in_fhandle: fails.\n");
  return -1;
}

static void remove_blk_from_fhandle(block_p b) {
  for (size_t i = 0; i < NUM_PAGES; i++)
    if (b->fhandle->blocks_in_mem[i] == b) {
      b->fhandle->blocks_in_mem[i] = 0;
      return;
    }
}

static int same_block(block_p b1, block_p b2) {
  if (!b1 || !b2) return 0;
  return ((strcmp(b1->fhandle->fname, b2->fhandle->fname) == 0)
          && (b1->blk_nr == b2->blk_nr));
}

static int is_last_block(block_p b) {
  return (b->blk_nr == b->fhandle->num_blocks - 1);
}

int peof(page_p p) {
  return (is_last_block(p->block) && eop(p));
}

/** Release a block (and unpinn). */
static void release_block(block_p b) {
  if (!b) return;
  if (b->page->pinned)
    unpin(b->page);
  remove_blk_from_fhandle(b);
  if (b->fhandle->current_block == b)
    b->fhandle->current_block = 0;
  b->page->block = 0;
  free(b);
}

void pager_terminate(void) {
  /* put_pqueues_info (DEBUG); */
  for (size_t i = 0; i < NUM_PAGES; i++) {
    if (!pages[i]) continue;
    release_block(pages[i]->block);
    if (pages[i]) {
      free(pages[i]->content);
      free(pages[i]);
    }
    pages[i] = 0;
  }
  for (size_t i = 0; i < MAX_OPEN_FILES; i++)
    close_tbl_file(file_handles[i]);
  q_unpinned = release_pqueue(q_unpinned);
  q_pinned = release_pqueue(q_pinned);
}

/* Find an available buffer page, in this order:
   - unused page,
   - unpinned page,
   - unpin a pinned page.
*/
static page_p available_page() {
  /* put_pqueues_info (DEBUG); */
  page_p pg;
  size_t i;
  /* First, get an unused page */
  if (q_pinned->len + q_unpinned->len < NUM_PAGES) {
    for (i = 0; pages[i]->block; i++);
    pg = pages[i];
  } else {
    /* put_msg (DEBUG, "available_page: all pages are used.\n"); */
    pg = pq_dequeue_unpinned(); /* replace an unpinned page */
    if (!pg)
      /* put_msg(DEBUG, "available_page: all pages are pinned.\n"); */
      /* If all pages are pinned, unpin the first page
         and get that one.
         This is of course a bad page replacement strategy. */
      pg = pq_dequeue_pinned();

    release_block(pg->block);
    init_page(pg);
  }
  pq_enqueue(q_unpinned, pg);
  return pg;
}

/* Returns the buffer page (position in pages[]) of the block).
   If the block does not have a buffer page allocated, allocate one.
*/
static page_p page_for_block(block_p b) {
  if (!b) return 0;
  for (size_t i = 0; i < NUM_PAGES; i++) {
    if (pages[i] && same_block(pages[i]->block, b))
      return pages[i];
  }
  /* put_msg(WARN, "block %d not in mem yet, allocate an available one.\n", b->blk_nr); */
  return available_page();
}

page_p get_page(char const* fname, int blknr) {
  block_p blk = 0;
  fhandle_p fh = get_tbl_file(fname);
  if (!fh) fh = open_tbl_file(fname);

  if (!fh) {
    put_msg(ERROR, "get_page: NULL fh.\n");
    return 0;
  }

  if (blknr == -1)
    blknr = fh->num_blocks == 0 ? 0 : fh->num_blocks - 1;

  if (blknr < 0 || blknr > fh->num_blocks) {
    put_msg(ERROR, "get_page: block nr %d out of ragne [0,%d].",
            blknr, fh->num_blocks);
    return 0;
  }

  if (fh->current_block && blknr == fh->current_block->blk_nr)
    blk = fh->current_block;
  else if (blknr == fh->num_blocks)
    fh->num_blocks++;
  else
    blk = get_buffered_blk_in_fhandle(fh, blknr);

  if (!blk) {
    blk = malloc(sizeof (block_struct));
    blk->fhandle = fh;
    blk->blk_nr = blknr;
    if (pin(blk) == NULL) {
      remove_blk_from_fhandle(blk);
      free(blk);
      return 0;
    }
    set_blk_in_fhandle(fh, blk);
    blk->page->current_pos = PAGE_HEADER_SIZE;
  }
  /* put_msg (DEBUG, "get_page: blk %d, page %d\n",
     blk->blk_nr, blk->page->page_nr); */
  fh->current_block = blk;
  return blk->page;
}

page_p get_page_for_append(char const* fname) {
  page_p pg = get_page(fname, -1);
  if (!pg) return 0;
  pg->current_pos = pg->free_pos;
  return pg;
}

page_p get_next_page(page_p p) {
  int blk_nr = is_last_block(p->block) ?
    p->block->fhandle->num_blocks : p->block->blk_nr + 1;
  return get_page(p->block->fhandle->fname, blk_nr);
}

void page_set_pos_begin(page_p p) {
  if (p)
    p->current_pos = PAGE_HEADER_SIZE;
}

page_p pin(block_p b) {
  if (!b) return 0;
  page_p pg = page_for_block(b);

  b->page = pg;
  pq_turn_pinned(pg);
  pg->block = b;
  pg->pinned = 1;
  if (!read_page(pg)) {
    put_msg(ERROR, "read_page %d fails\n", pg->page_nr);
    return 0;
  }
  return pg;
}

void unpin(page_p pg) {
  pq_turn_unpinned(pg);
  pg->pinned = 0;
  if (pg->dirty != 0)
    write_page(pg);
}

int read_page(page_p p) {
  if (!p) {
    put_msg(ERROR, "read_page: NULL page.\n");
    return 0;
  }
  if (p->dirty) return 1;
  if (!p->block) {
    put_msg(ERROR, "read_page: NULL block.\n");
    return 0;
  }
  if (!p->block->fhandle) {
    put_msg(ERROR, "read_page: NULL fhandle.\n");
    return 0;
  }
  int fd = p->block->fhandle->fd;
  if (lseek(fd, (off_t) BLOCK_SIZE * p->block->blk_nr, SEEK_SET) < 0) {
    put_msg(ERROR, "read_page: lseek to fd %d offset %ld fails.\n",
            fd, BLOCK_SIZE * p->block->blk_nr);
    return 0;
  }
  int bytes_read = read(fd, p->content, BLOCK_SIZE);
  if (bytes_read == -1) return 0;
  if (bytes_read == 0)
    set_page_free_pos(p, PAGE_HEADER_SIZE);
  else {
    inc_num_reads(fd, p->block->blk_nr);
    check_page_header_size(p);
    set_page_free_pos_from_content(p);
  }
  return 1;
}

int write_page(page_p p) {
  if (!p) {
    put_msg(ERROR, "write_page: NULL page.\n");
    return 0;
  }
  if (!p->dirty) return 1;
  if (!p->block) return 0;
  if (!p->block->fhandle) return 0;

  int fd = p->block->fhandle->fd;

  if (lseek(fd, (off_t) BLOCK_SIZE * p->block->blk_nr, SEEK_SET) < 0)
    return 0;
  inc_num_writes(fd, p->block->blk_nr);
  p->dirty = 0;
  if (write(fd, p->content, BLOCK_SIZE) == -1) return 0;
  return 1;
}

int page_block_nr(page_p p) {
  if (!p) {
    put_msg(ERROR, "page_block_nr: NULL page.\n");
    return -1;
  }
  return p->block->blk_nr;
}

int page_current_pos(page_p p) {
  if (!p) {
    put_msg(ERROR, "page_current_pos: NULL page.\n");
    return -1;
  }
  return p->current_pos;
}

int page_set_current_pos(page_p p, int pos) {
  if (!p) {
    put_msg(ERROR, "page_set_current_pos: NULL page.\n");
    return -1;
  }
  p->current_pos = pos;
  return p->current_pos;
}

int page_valid_pos_for_get(page_p p, int offset) {
  if (offset >= PAGE_HEADER_SIZE && offset < p->free_pos)
    return 1;
  put_msg(WARN,
          "page_valid_pos_for_get: page: %d, offset %d out of range [%ld,%ld]\n",
          p->page_nr, offset, PAGE_HEADER_SIZE, (p->free_pos) - 1);
  return 0;
}

int page_valid_pos_for_put(page_p p, int offset, int len) {
  if (offset >= PAGE_HEADER_SIZE && offset <= p->free_pos
      && offset <= BLOCK_SIZE - len)
    return 1;
  return 0;
}

/** Called after a successful put, so there is no need to check if
    current position is valid */
static void set_pos_after_put(page_p p, int offset) {
  if (offset > p->free_pos) {
    set_page_free_pos(p, offset);
  }
  p->current_pos = offset;
}

int eop(page_p p) {
  return (p->current_pos >= p->free_pos);
}

int page_get_int(page_p p) {
  if (!page_valid_pos_for_get(p, p->current_pos)) {
    put_msg(FATAL, "page_get_int\n");
    exit(EXIT_FAILURE);
  }
  int res = (int) *((int *)((p->content) + p->current_pos));
  p->current_pos += INT_SIZE;
  return res;
}

int page_put_int(page_p p, int val) {
  if (!page_valid_pos_for_put(p, p->current_pos, INT_SIZE)) {
    return 0;
  }
  memcpy(p->content + p->current_pos, (char *) &val, INT_SIZE);
  p->dirty = 1;
  set_pos_after_put(p, p->current_pos + INT_SIZE);
  return 1;
}

int page_get_int_at(page_p p, int offset) {
  if (!page_valid_pos_for_get(p, offset)) {
    put_msg(FATAL, "page_get_int_at\n");
    exit(EXIT_FAILURE);
  }
  int res = (int) *((int *)((p->content) + offset));
  p->current_pos += INT_SIZE;
  return res;
}

int page_put_int_at(page_p p, int offset, int val) {
  if (!page_valid_pos_for_put(p, offset, INT_SIZE)) {
    return 0;
  }
  memcpy(p->content + offset, (char *) &val, INT_SIZE);
  p->dirty = 1;
  set_pos_after_put(p, offset + INT_SIZE);
  return 1;
}

int page_get_str(page_p p, char* str, int len) {
  if (!page_valid_pos_for_get(p, p->current_pos)) {
    put_msg(FATAL, "page_get_str\n");
    exit(EXIT_FAILURE);
  }
  strncpy(str, p->content + p->current_pos, len);
  p->current_pos += len;
  return 0;
}

int page_put_str(page_p p, char const* str, int len) {
  if (!page_valid_pos_for_put(p, p->current_pos, len)) {
    return 0;
  }
  strncpy(p->content + p->current_pos, str, len);
  p->dirty = 1;
  set_pos_after_put(p, p->current_pos + len);
  return 1;
}

int page_get_str_at(page_p p, int offset, char* str, int len) {
  if (!page_valid_pos_for_get(p, offset)) {
    put_msg(FATAL, "page_get_str_at\n");
    exit(EXIT_FAILURE);
  }
  strncpy(str, p->content + offset, len);
  p->current_pos += len;
  return 0;
}

int page_put_str_at(page_p p, int offset, char const* str, int len) {
  if(!page_valid_pos_for_put(p, offset, len)) {
    return 0;
  }
  strncpy(p->content + offset, str, len);
  p->dirty = 1;
  set_pos_after_put(p, offset + len);
  return 1;
}
