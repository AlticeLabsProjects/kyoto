#!/bin/sh

### BEGIN INIT INFO
# Provides:        kyoto
# Required-Start:  $network
# Required-Stop:   $network
# Default-Start:   2 3 4 5
# Default-Stop:    0 1 6
# Short-Description: Start Kyoto Tycoon daemon
### END INIT INFO

if [ -r /etc/default/kyoto ]; then
	. /etc/default/kyoto
fi

PATH=/sbin:/bin:/usr/sbin:/usr/bin

. /lib/lsb/init-functions

DAEMON=/usr/bin/ktserver
test -x $DAEMON || exit 5

RUNASUSER=kyoto

mkdir -m 0755 -p /var/run/kyoto
chown ${RUNASUSER}:${RUNASUSER} /var/run/kyoto

PIDFILE=/var/run/kyoto/kyoto.pid
LOGFILE=/var/log/kyoto/kyoto.log

KTSERVER_OPTS="-dmn -pid $PIDFILE -log $LOGFILE $KTSERVER_OPTS"

case $1 in
	start)
		log_daemon_msg "Starting Kyoto Tycoon server" "kyoto"
		start-stop-daemon --start --quiet --pidfile $PIDFILE --chuid $RUNASUSER --startas $DAEMON -- $KTSERVER_OPTS
		$0 status >/dev/null
		log_end_msg $?
		;;
	stop)
		log_daemon_msg "Stopping Kyoto Tycoon server" "kyoto"
		start-stop-daemon --stop --quiet --pidfile $PIDFILE
		log_end_msg $?
		rm -f $PIDFILE
		;;
	restart|force-reload)
		$0 stop && sleep 2 && $0 start
		;;
	try-restart)
		if $0 status >/dev/null; then
			$0 restart
		else
			exit 0
		fi
		;;
	reload)
		exit 3
		;;
	status)
		status_of_proc $DAEMON "Kyoto Tycoon server"
		;;
	*)
		echo "Usage: $0 {start|stop|restart|try-restart|force-reload|status}"
		exit 2
		;;
esac
