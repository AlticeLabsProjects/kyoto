#include <kcpolydb.h>
#include <kcdbext.h>

using namespace std;
using namespace kyotocabinet;

// main routine
int main(int argc, char** argv) {

  // create the database object
  PolyDB db;

  // open the database
  if (!db.open()) {
    cerr << "open error: " << db.error().name() << endl;
  }

  // store records
  db.set("1", "this is a pen");
  db.set("2", "what a beautiful pen this is");
  db.set("3", "she is beautiful");

  // define the mapper and the reducer
  class MapReduceImpl : public MapReduce {
    // call back function of the mapper
    bool map(const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz) {
      vector<string> words;
      strsplit(string(vbuf, vsiz), ' ', &words);
      for (vector<string>::iterator it = words.begin();
           it != words.end(); it++) {
        emit(it->data(), it->size(), "", 0);
      }
      return true;
    }
    // call back function of the reducer
    bool reduce(const char* kbuf, size_t ksiz, ValueIterator* iter) {
      size_t count = 0;
      const char* vbuf;
      size_t vsiz;
      while ((vbuf = iter->next(&vsiz)) != NULL) {
        count++;
      }
      cout << string(kbuf, ksiz) << ": " << count << endl;
      return true;
    }
  } mr;

  // execute the MapReduce process
  if (!mr.execute(&db)) {
    cerr << "MapReduce error: " << db.error().name() << endl;
  }

  // close the database
  if (!db.close()) {
    cerr << "close error: " << db.error().name() << endl;
  }

  return 0;
}
