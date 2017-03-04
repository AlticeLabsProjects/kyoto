%define selinux_policy_ver 3.13.1-102

%define kt_version __KT_VERSION_PLACEHOLDER__
%define kt_timestamp %(date +"%Y%m%d")
%define kt_installdir /usr

%define relabel_files() \
/usr/sbin/restorecon -R %{kt_installdir}/bin/ktserver; \
/usr/sbin/restorecon -R %{kt_installdir}/libexec/ktplug*.so; \
/usr/sbin/restorecon -R %{_unitdir}/kyoto.service; \
/usr/sbin/restorecon -R /var/lib/kyoto; \
/usr/sbin/restorecon -R /var/log/kyoto; \

Name: kyoto-tycoon-selinux
Version: %{kt_version}
Release: %{kt_timestamp}%{?dist}
Summary: SELinux policy module for Kyoto Tycoon

License: GPL
URL: https://github.com/alticelabs/kyoto
Source0: kyoto.te
Source1: kyoto.fc
Source2: kyoto.if

BuildRequires: selinux-policy-devel >= %{selinux_policy_ver}
Requires: kyoto-tycoon = %{kt_version}, systemd, selinux-policy-base >= %{selinux_policy_ver}, selinux-policy-targeted >= %{selinux_policy_ver}, policycoreutils, libselinux-utils
BuildArch: noarch

%description
This package installs an hardened SELinux policy module for the Kyoto Tycoon key-value server.


%prep
cp -p %{SOURCE0} .
cp -p %{SOURCE1} .
cp -p %{SOURCE2} .


%build
make -f /usr/share/selinux/devel/Makefile kyoto.pp


%install
install -d %{buildroot}%{_datadir}/selinux/packages
install -m 644 kyoto.pp %{buildroot}%{_datadir}/selinux/packages


%post
/usr/sbin/semodule -n -s targeted -i %{_datadir}/selinux/packages/kyoto.pp

if [ $1 == 1 ]; then  # ...do not run on upgrade.
	/usr/sbin/semanage port -a -t kyoto_port_t -p tcp 1978
fi

if /usr/sbin/selinuxenabled; then
	/usr/sbin/load_policy
	%relabel_files

	/usr/bin/systemctl daemon-reexec
	/usr/bin/systemctl try-restart kyoto
fi


%postun
if [ $1 == 0 ]; then  # ...do not run on upgrade.
	/usr/sbin/semanage port -d -t kyoto_port_t -p tcp 1978
	/usr/sbin/semodule -n -r kyoto >/dev/null

	if /usr/sbin/selinuxenabled; then
		/usr/sbin/load_policy
		%relabel_files

		/usr/bin/systemctl daemon-reexec
		/usr/bin/systemctl try-restart kyoto
	fi
fi


%files
%attr(0600,root,root) %{_datadir}/selinux/packages/kyoto.pp
