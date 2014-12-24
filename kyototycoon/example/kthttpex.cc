#include <kthttp.h>

using namespace std;
using namespace kyototycoon;

// the flag whether the server is alive
HTTPServer* g_serv = NULL;

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
  class Worker : public HTTPServer::Worker {
    int32_t process(HTTPServer* serv, HTTPServer::Session* sess,
                    const string& path, HTTPClient::Method method,
                    const map<string, string>& reqheads,
                    const string& reqbody,
                    map<string, string>& resheads,
                    string& resbody,
                    const map<string, string>& misc) {
      // echo back the input data
      for (map<string, string>::const_iterator it = reqheads.begin();
           it != reqheads.end(); it++) {
        if (!it->first.empty()) resbody.append(it->first + ": ");
        resbody.append(it->second + "\n");
      }
      resbody.append(reqbody);
      // return the status code
      return 200;
    }
  };
  Worker worker;

  // prepare the server
  HTTPServer serv;
  serv.set_network("127.0.0.1:1978", 1.0);
  serv.set_worker(&worker, 4);
  g_serv = &serv;

  // start the server and block until its stop
  serv.start();

  // clean up connections and other resources
  serv.finish();

  return 0;
}
