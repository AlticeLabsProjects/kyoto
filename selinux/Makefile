#
# Makefile (for testing the SELinux policy without packaging)
#


all: kyoto.pp

kyoto.pp: kyoto.te kyoto.fc kyoto.if
	$(MAKE) -f /usr/share/selinux/devel/Makefile $@

install: all
	sudo semodule -s targeted -i kyoto.pp
	sudo semanage port -l | egrep 'kyoto.*1978' >/dev/null || sudo semanage port -a -t kyoto_port_t -p tcp 1978

	# Our "kyoto-tycoon" packages install into "/usr" and the SELinux policy assumes this.
	sudo restorecon -F -R -v /usr/bin/ktserver
	sudo restorecon -F -R -v /usr/libexec/ktplug*.so
	sudo restorecon -F -R -v /usr/lib/systemd/system/kyoto.service
	sudo restorecon -F -R -v /var/lib/kyoto
	sudo restorecon -F -R -v /var/log/kyoto

	# Restart systemd to ensure it creates things with the correct labels...
	test -x /usr/bin/systemctl && sudo /usr/bin/systemctl daemon-reexec

clean:
	rm -f *.pp
	rm -rf tmp


.PHONY: all install clean


# EOF - Makefile
