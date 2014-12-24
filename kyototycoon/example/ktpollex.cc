#include <ktsocket.h>

using namespace std;
using namespace kyototycoon;

// the flag whether the server is alive
Poller* g_poll = NULL;

// stop the running server
static void stopserver(int signum) {
  if (g_poll) g_poll->abort();
  g_poll = NULL;
}

// main routine
int main(int argc, char** argv) {

  // set the signal handler to stop the server
  setkillsignalhandler(stopserver);

  // open the server socket
  ServerSocket serv;
  serv.open("127.0.0.1:1978");

  // open the event notifier
  Poller poll;
  poll.open();
  g_poll = &poll;

  // deposit the server socket into the poller
  serv.set_event_flags(Pollable::EVINPUT);
  poll.deposit(&serv);

  // event loop
  while (g_poll) {
    // wait one or more active requests
    if (poll.wait()) {
      // iterate all active requests
      Pollable* event;
      while ((event = poll.next()) != NULL) {
        if (event == &serv) {
          // accept a new connection
          Socket* sock = new Socket;
          sock->set_timeout(1.0);
          if (serv.accept(sock)) {
            sock->set_event_flags(Pollable::EVINPUT);
            poll.deposit(sock);
          }
          // undo the server socket
          serv.set_event_flags(Pollable::EVINPUT);
          poll.undo(&serv);
        } else {
          // process each request
          Socket* sock = (Socket*)event;
          // read a line from the client socket
          char line[1024];
          if (sock->receive_line(line, sizeof(line))) {
            if (!kc::stricmp(line, "/quit")) {
              // process the quit command
              sock->printf("> Bye!\n");
              poll.withdraw(sock);
              sock->close();
              delete sock;
            } else {
              // echo back the message
              sock->printf("> %s\n", line);
              // undo the client socket
              serv.set_event_flags(Pollable::EVINPUT);
              poll.undo(sock);
            }
          } else {
            // process a connection closed by the client
            poll.withdraw(sock);
            sock->close();
            delete sock;
          }
        }
      }
    }
  }

  // clean up all connections
  if (poll.flush()) {
    Pollable* event;
    while ((event = poll.next()) != NULL) {
      if (event != &serv) {
        Socket* sock = (Socket*)event;
        poll.withdraw(sock);
        sock->close();
        delete sock;
      }
    }
  }

  // close the event notifier
  poll.close();

  // close the server socket
  serv.close();

  return 0;
}
