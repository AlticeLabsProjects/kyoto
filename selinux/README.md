Kyoto Tycoon SELinux Policy
===========================

**Note:** If you're using RHEL or CentOS, run `make rpm-selinux` from the top directory and install the resulting `.rpm` instead. The `Makefile` in this directory is meant for testing the policy while developing it, not for user consumption.

Having said this, you _may_ be able to install this policy on any system supporting SELinux, provided you install Kyoto Tycoon into `/usr`, have all the necessary SELinux commands installed, and your system uses `systemd` (you might get around this last one by editing the `Makefile`).

Things you may want to do while using this policy:

For convenience, `ktserver` can open outbound connections to `TCP/1978` by default. If you're not using it as a replication slave, it doesn't need this and you can block it for safety:
```
setsebool kyoto_connect_to_replication_master false
```

If you want to run `ktserver` on some port other than `TCP/1978`, add it to the default set:
```
semanage port -a -t kyoto_port_t -p tcp 12345
```
