#include <kcpolydb.h>

using namespace std;
using namespace kyotocabinet;

// main routine
int main(int argc, char** argv) {

  // create the database object
  PolyDB db;

  // open the database
  if (!db.open("casket.kch", PolyDB::OREADER)) {
    cerr << "open error: " << db.error().name() << endl;
  }

  // define the visitor
  class VisitorImpl : public DB::Visitor {
    // call back function for an existing record
    const char* visit_full(const char* kbuf, size_t ksiz,
                           const char* vbuf, size_t vsiz, size_t *sp) {
      cout << string(kbuf, ksiz) << ":" << string(vbuf, vsiz) << endl;
      return NOP;
    }
    // call back function for an empty record space
    const char* visit_empty(const char* kbuf, size_t ksiz, size_t *sp) {
      cerr << string(kbuf, ksiz) << " is missing" << endl;
      return NOP;
    }
  } visitor;

  // retrieve a record with visitor
  if (!db.accept("foo", 3, &visitor, false) ||
      !db.accept("dummy", 5, &visitor, false)) {
    cerr << "accept error: " << db.error().name() << endl;
  }

  // traverse records with visitor
  if (!db.iterate(&visitor, false)) {
    cerr << "iterate error: " << db.error().name() << endl;
  }

  // close the database
  if (!db.close()) {
    cerr << "close error: " << db.error().name() << endl;
  }

  return 0;
}
