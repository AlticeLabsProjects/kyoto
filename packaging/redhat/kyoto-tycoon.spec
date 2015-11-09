%define kt_version __KT_VERSION_PLACEHOLDER__
%define kt_timestamp %(date +"%Y%m%d")
%define kt_installdir /usr

# Use systemd units instead of SysV initscripts on newer distributions...
%define use_systemd ((0%{?fedora} && 0%{?fedora} >= 18) || (0%{?rhel} && 0%{?rhel} >= 7))

# Needed to avoid unresolvable dependencies on RHEL7...
%define debug_package %{nil}

Name: kyoto-tycoon
Version: %{kt_version}
Release: %{kt_timestamp}%{?dist}
Summary: Kyoto Tycoon key-value server (and Kyoto Cabinet library)

License: GPL
URL: https://github.com/sapo/kyoto
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


%prep
%setup -c


%build
make PREFIX=%{kt_installdir}


%install
rm -rf %{buildroot}
make install DESTDIR=%{buildroot}

if echo %{_libdir} | /bin/grep -q 64; then
	# Otherwise the runtime linker won't find the libraries...
	mv %{buildroot}/usr/lib %{buildroot}/usr/lib64
fi

%{__mkdir_p} ${RPM_BUILD_ROOT}/var/lib/kyoto

%if %{use_systemd}
%{__mkdir_p} ${RPM_BUILD_ROOT}%{_unitdir}
%{__install} -m0644 packaging/redhat/kyoto.service ${RPM_BUILD_ROOT}%{_unitdir}/kyoto.service
%else
%{__mkdir_p} ${RPM_BUILD_ROOT}%{_sysconfdir}/init.d
%{__install} -m0755 packaging/redhat/kyoto-init.sh ${RPM_BUILD_ROOT}%{_sysconfdir}/init.d/kyoto
%endif

%{__mkdir_p} ${RPM_BUILD_ROOT}%{_sysconfdir}/default
%{__install} -m0644 packaging/scripts/kyoto.conf ${RPM_BUILD_ROOT}%{_sysconfdir}/default/kyoto


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
if [ $1 -gt 1 ]; then  # ...do nothing on upgrade.
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
/usr/bin/systemctl stop kyoto.service 2>&1
%else
/sbin/service kyoto stop >/dev/null 2>&1
%endif
/usr/bin/pkill -KILL -U kyoto || /bin/true
%if %{use_systemd}
/usr/bin/systemctl disable kyoto.service 2>&1
%else
/sbin/chkconfig --del kyoto
%endif


%postun
if [ $1 -gt 0 ]; then  # ...do nothing on upgrade.
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
%doc LICENSE README.md
%dir %attr(-,kyoto,kyoto) /var/lib/kyoto
%config(noreplace) %{_sysconfdir}/default/kyoto
%{kt_installdir}/bin/*
%{kt_installdir}/include/*
%{kt_installdir}/lib*/*
%{kt_installdir}/share/doc/*
%{kt_installdir}/share/man/man1/*

%if !%{use_systemd}
%{_sysconfdir}/init.d/kyoto
%endif


%changelog

