#
# kyoto-tycoon.spec - RPM packages for RHEL/CentOS 6 and 7.
#

%define kt_version __KT_VERSION_PLACEHOLDER__
%define kt_timestamp %(date +"%Y%m%d")
%define kt_installdir %{_usr}

# Use systemd units instead of SysV initscripts on newer distributions...
%define use_systemd ((0%{?fedora} && 0%{?fedora} >= 18) || (0%{?rhel} && 0%{?rhel} >= 7))

# Needed to avoid unresolvable dependencies on RHEL7...
%define debug_package %{nil}

Name: kyoto-tycoon
Version: %{kt_version}
Release: %{kt_timestamp}%{?dist}
Summary: Kyoto Tycoon key-value server (and Kyoto Cabinet library)

License: GPLv3
URL: https://github.com/alticelabs/kyoto
Source0: kyoto-%{kt_timestamp}.tar.gz

BuildRequires: lua-devel, zlib-devel, lzo-devel
Requires: /bin/grep, /bin/true, /bin/false
Requires(pre): /usr/sbin/useradd, /usr/sbin/groupadd
Requires(preun): /usr/bin/pkill
Requires(postun): /usr/sbin/userdel, /usr/sbin/groupdel

%if %{use_systemd}
BuildRequires: systemd
Requires: systemd
%else
Requires: initscripts, chkconfig, redhat-lsb-core
%endif

%description
Kyoto Tycoon is a lightweight server on top of the Kyoto Cabinet
key-value database, built for high-performance and concurrency.

Some of its features include:

  - master-slave and master-master replication
  - in-memory and persistent databases
  - hash and tree-based database formats
  - server-side scripting in Lua
  - support for the memcached protocol


%package devel

Summary: Kyoto Tycoon (and Kyoto Cabinet library) development files
Requires: kyoto-tycoon = %{kt_version}

%description devel

Kyoto Tycoon is a lightweight server on top of the Kyoto Cabinet
key-value database, built for high-performance and concurrency.

This package contains header files and static libraries for the
Kyoto Tycoon server and the bundled Kyoto Cabinet library.


%package doc

Summary: Kyoto Tycoon (and Kyoto Cabinet library) documentation
BuildArch: noarch
Requires: kyoto-tycoon = %{kt_version}

%description doc

Kyoto Tycoon is a lightweight server on top of the Kyoto Cabinet
key-value database, built for high-performance and concurrency.

This package contains additional documentation for the Kyoto Tycoon
server and the API reference for the bundled Kyoto Cabinet library.


%prep
%setup -c


%build
make PREFIX=%{kt_installdir}


%install
rm -rf %{buildroot}
make install DESTDIR=%{buildroot}

if echo %{_libdir} | /bin/grep -q 64; then
	# Otherwise the runtime linker won't find the libraries...
	mv %{buildroot}%{kt_installdir}/lib %{buildroot}%{kt_installdir}/lib64
fi

%{__mkdir_p} ${RPM_BUILD_ROOT}/var/lib/kyoto
%{__mkdir_p} ${RPM_BUILD_ROOT}/var/log/kyoto

%if %{use_systemd}
%{__mkdir_p} ${RPM_BUILD_ROOT}%{_unitdir}
%{__install} -m0644 packaging/redhat/kyoto.service ${RPM_BUILD_ROOT}%{_unitdir}/kyoto.service
%else
%{__mkdir_p} ${RPM_BUILD_ROOT}%{_sysconfdir}/init.d
%{__install} -m0755 packaging/redhat/kyoto-init.sh ${RPM_BUILD_ROOT}%{_sysconfdir}/init.d/kyoto
%endif

%{__mkdir_p} ${RPM_BUILD_ROOT}%{_sysconfdir}/default
%{__install} -m0644 packaging/scripts/kyoto.conf ${RPM_BUILD_ROOT}%{_sysconfdir}/default/kyoto

%{__mkdir_p} ${RPM_BUILD_ROOT}%{_sysconfdir}/logrotate.d
%{__install} -m0644 packaging/scripts/logrotate.conf ${RPM_BUILD_ROOT}%{_sysconfdir}/logrotate.d/kyoto

%pre
if [ $1 -gt 1 ]; then  # ...do nothing on upgrade.
	exit 0
fi

if ! /bin/grep -q kyoto /etc/group; then
	/usr/sbin/groupadd -r kyoto
fi

if ! /bin/grep -q kyoto /etc/passwd; then
	/usr/sbin/useradd -r -M -d /var/lib/kyoto -g kyoto -s /bin/false kyoto
fi


%post
%if %{use_systemd}
/usr/bin/systemctl daemon-reload
/usr/bin/systemctl try-restart kyoto.service
%else
/sbin/service kyoto try-restart >/dev/null 2>&1
%endif

if [ $1 -gt 1 ]; then  # ...do nothing else on upgrade.
	exit 0
fi

%if !%{use_systemd}
/sbin/chkconfig --add kyoto
%endif


%preun
if [ $1 -gt 0 ]; then  # ...do nothing on upgrade.
	exit 0
fi

%if %{use_systemd}
/usr/bin/systemctl stop kyoto.service >/dev/null 2>&1
%else
/sbin/service kyoto stop >/dev/null 2>&1
%endif
/usr/bin/pkill -KILL -U kyoto || /bin/true
%if %{use_systemd}
/usr/bin/systemctl disable kyoto.service >/dev/null 2>&1
%else
/sbin/chkconfig --del kyoto
%endif


%postun
%if %{use_systemd}
/usr/bin/systemctl daemon-reload
%endif

if [ $1 -gt 0 ]; then  # ...do nothing else on upgrade.
	exit 0
fi

if /bin/grep -q kyoto /etc/passwd; then
	/usr/sbin/userdel kyoto
fi

if /bin/grep -q kyoto /etc/group; then
	/usr/sbin/groupdel kyoto
fi


%clean
rm -rf %{buildroot}


%files
%defattr(-,root,root,-)
%doc LICENSE
%dir %attr(-,kyoto,kyoto) /var/lib/kyoto
%dir %attr(-,kyoto,kyoto) /var/log/kyoto
%config(noreplace) %{_sysconfdir}/default/kyoto
%config(noreplace) %{_sysconfdir}/logrotate.d/kyoto
%{kt_installdir}/bin/*
%{kt_installdir}/lib*/*.so*
%{kt_installdir}/share/man/man1/*

%if %{use_systemd}
%{_unitdir}/kyoto.service
%else
%{_sysconfdir}/init.d/kyoto
%endif


%files devel
%defattr(-,root,root,-)
%{kt_installdir}/include/*
%{kt_installdir}/lib*/*.a
%{kt_installdir}/lib*/pkgconfig/*


%files doc
%defattr(-,root,root,-)
%doc README.md
%{kt_installdir}/share/doc/kyotocabinet/*
%{kt_installdir}/share/doc/kyototycoon/*


%changelog

