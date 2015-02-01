%define kt_version __KT_VERSION_PLACEHOLDER__
%define kt_timestamp %(date +"%Y%m%d")
%define kt_installdir /usr

Name: kyoto-tycoon
Version: %{kt_version}
Release: %{kt_timestamp}%{?dist}
Summary: Kyoto Tycoon key-value server (and Kyoto Cabinet library)	

License: GPL
URL: https://github.com/sapo/kyoto
Source0: kyoto-%{kt_timestamp}.tar.gz

BuildRequires: lua-devel, zlib-devel
Requires(pre): /usr/sbin/useradd
Requires(post): chkconfig
Requires(preun): chkconfig, initscripts
Requires(postun): /usr/sbin/userdel

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

if echo %{_libdir} | grep -q 64; then
	# Otherwise the runtime linker won't find the libraries...
	mv %{buildroot}/usr/lib %{buildroot}/usr/lib64
fi

%{__mkdir_p} ${RPM_BUILD_ROOT}%{_sharedstatedir}/kyoto

%{__mkdir_p} ${RPM_BUILD_ROOT}%{_sysconfdir}/init.d
%{__install} -m0755 scripts/redhat-init.sh ${RPM_BUILD_ROOT}%{_sysconfdir}/init.d/kyoto

%{__mkdir_p} ${RPM_BUILD_ROOT}%{_sysconfdir}/default
%{__install} -m0644 scripts/kyoto.conf ${RPM_BUILD_ROOT}%{_sysconfdir}/default/kyoto


%pre
if ! grep -q kyoto /etc/group; then
	/usr/sbin/groupadd -r kyoto
fi

if ! grep -q kyoto /etc/passwd; then
	/usr/sbin/useradd -r -M -d /var/lib/kyoto -g kyoto -s /bin/false kyoto
fi


%post
/sbin/chkconfig --add kyoto
chown kyoto:kyoto %{_sharedstatedir}/kyoto


%preun
if [ $1 -eq 0 ]; then
    /sbin/service kyoto stop >/dev/null 2>&1
    /sbin/chkconfig --del kyoto
fi


%postun
/usr/sbin/userdel kyoto


%clean
rm -rf %{buildroot}


%files
%defattr(-,root,root,-)
%doc LICENSE README.md
%{kt_installdir}/*
%{_sharedstatedir}/kyoto
%{_sysconfdir}/init.d/kyoto
%config(noreplace) %{_sysconfdir}/default/kyoto


%changelog

