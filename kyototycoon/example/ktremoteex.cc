#include <ktremotedb.h>

using namespace std;
using namespace kyototycoon;

// main routine
int main(int argc, char** argv) {

  // create the database object
  RemoteDB db;

  // open the database
  if (!db.open()) {
    cerr << "open error: " << db.error().name() << endl;
  }

  // store records
  if (!db.set("foo", "hop") ||
      !db.set("bar", "step") ||
      !db.set("baz", "jump")) {
    cerr << "set error: " << db.error().name() << endl;
  }

  // retrieve a record
  string value;
  if (db.get("foo", &value)) {
    cout << value << endl;
  } else {
    cerr << "get error: " << db.error().name() << endl;
  }

  // traverse records
  RemoteDB::Cursor* cur = db.cursor();
  cur->jump();
  string ckey, cvalue;
  while (cur->get(&ckey, &cvalue, NULL, true)) {
    cout << ckey << ":" << cvalue << endl;
  }
  delete cur;

  // close the database
  if (!db.close()) {
    cerr << "close error: " << db.error().name() << endl;
  }

  return 0;
}
