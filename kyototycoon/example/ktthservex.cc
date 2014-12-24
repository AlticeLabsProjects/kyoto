#include <ktthserv.h>

using namespace std;
using namespace kyototycoon;

// the flag whether the server is alive
ThreadedServer* g_serv = NULL;

// stop the running server
static void stopserver(int signum) {
  if (g_serv) g_serv->stop();
  g_serv = NULL;
}

// main routine
int main(int argc, char** argv) {

  // set the signal handler to stop the server
  setkillsignalhandler(stopserver);

  // prepare the worker
  class Worker : public ThreadedServer::Worker {
    bool process(ThreadedServer* serv, ThreadedServer::Session* sess) {
      bool keep = false;
      // read a line from the client socket
      char line[1024];
      if (sess->receive_line(line, sizeof(line))) {
        if (!kc::stricmp(line, "/quit")) {
          // process the quit command
          sess->printf("> Bye!\n");
        } else {
          // echo back the message
          sess->printf("> %s\n", line);
          keep = true;
        }
      }
      return keep;
    }
  };
  Worker worker;

  // prepare the server
  ThreadedServer serv;
  serv.set_network("127.0.0.1:1978", 1.0);
  serv.set_worker(&worker, 4);
  g_serv = &serv;

  // start the server and block until its stop
  serv.start();

  // clean up connections and other resources
  serv.finish();

  return 0;
}
