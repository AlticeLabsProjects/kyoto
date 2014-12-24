#include <kclangc.h>

/* call back function for an existing record */
const char* visitfull(const char* kbuf, size_t ksiz,
                      const char* vbuf, size_t vsiz, size_t *sp, void* opq) {
  fwrite(kbuf, 1, ksiz, stdout);
  printf(":");
  fwrite(vbuf, 1, vsiz, stdout);
  printf("\n");
  return KCVISNOP;
}

/* call back function for an empty record space */
const char* visitempty(const char* kbuf, size_t ksiz, size_t *sp, void* opq) {
  fwrite(kbuf, 1, ksiz, stdout);
  printf(" is missing\n");
  return KCVISNOP;
}

/* main routine */
int main(int argc, char** argv) {
  KCDB* db;
  KCCUR* cur;
  char *kbuf, *vbuf;
  size_t ksiz, vsiz;
  const char *cvbuf;

  /* create the database object */
  db = kcdbnew();

  /* open the database */
  if (!kcdbopen(db, "casket.kch", KCOWRITER | KCOCREATE)) {
    fprintf(stderr, "open error: %s\n", kcecodename(kcdbecode(db)));
  }

  /* store records */
  if (!kcdbset(db, "foo", 3, "hop", 3) ||
      !kcdbset(db, "bar", 3, "step", 4) ||
      !kcdbset(db, "baz", 3, "jump", 4)) {
    fprintf(stderr, "set error: %s\n", kcecodename(kcdbecode(db)));
  }

  /* retrieve a record */
  vbuf = kcdbget(db, "foo", 3, &vsiz);
  if (vbuf) {
    printf("%s\n", vbuf);
    kcfree(vbuf);
  } else {
    fprintf(stderr, "get error: %s\n", kcecodename(kcdbecode(db)));
  }

  /* traverse records */
  cur = kcdbcursor(db);
  kccurjump(cur);
  while ((kbuf = kccurget(cur, &ksiz, &cvbuf, &vsiz, 1)) != NULL) {
    printf("%s:%s\n", kbuf, cvbuf);
    kcfree(kbuf);
  }
  kccurdel(cur);

  /* retrieve a record with visitor */
  if (!kcdbaccept(db, "foo", 3, visitfull, visitempty, NULL, 0) ||
      !kcdbaccept(db, "dummy", 5, visitfull, visitempty, NULL, 0)) {
    fprintf(stderr, "accept error: %s\n", kcecodename(kcdbecode(db)));
  }

  /* traverse records with visitor */
  if (!kcdbiterate(db, visitfull, NULL, 0)) {
    fprintf(stderr, "iterate error: %s\n", kcecodename(kcdbecode(db)));
  }

  /* close the database */
  if (!kcdbclose(db)) {
    fprintf(stderr, "close error: %s\n", kcecodename(kcdbecode(db)));
  }

  /* delete the database object */
  kcdbdel(db);

  return 0;
}
