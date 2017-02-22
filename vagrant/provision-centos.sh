#!/bin/bash -e
#
# Provision CentOS VM (vagrant shell provisioner).
#


if [ "$(id -u)" != "$(id -u vagrant)" ]; then
    echo "The provisioning script must be run as the \"vagrant\" user!" >&2
    exit 1
fi


echo "provision-centos.sh: Customizing the base system..."

CENTOS_RELEASE=$(rpm -q --queryformat '%{VERSION}' centos-release)

sudo rpm --import "/etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-$CENTOS_RELEASE"

sudo yum -q -y clean all
sudo yum -q -y makecache fast

sudo yum -q -y install \
    avahi chrony mlocate net-tools lsof iotop sysstat \
    htop nmap-ncat ntpdate pv tree vim ltrace strace

# Minor cleanup...
sudo systemctl stop tuned firewalld
sudo systemctl -q disable tuned firewalld

# Set a local timezone (the default for CentOS boxes is EDT)...
sudo timedatectl set-timezone "Europe/Lisbon"

sudo systemctl -q enable chronyd
sudo systemctl start chronyd

# This gives us an easly reachable ".local" name for the VM...
sudo systemctl -q enable avahi-daemon
sudo systemctl start avahi-daemon

# Prevent locale from being forwarded from the host, causing issues...
if sudo grep -q '^AcceptEnv\s.*LC_' /etc/ssh/sshd_config; then
    sudo sed -i 's/^\(AcceptEnv\s.*LC_\)/#\1/' /etc/ssh/sshd_config
    sudo systemctl restart sshd
fi

# Generate the initial "locate" DB...
if sudo test -x /etc/cron.daily/mlocate; then
    sudo /etc/cron.daily/mlocate
fi


echo "provision-centos.sh: Running project-specific actions..."

# Install extra packages needed for the project...
sudo yum -q -y install \
    gcc gcc-c++ automake \
    zlib-devel lzo-devel lua-devel \
    rpmdevtools \
    selinux-policy-devel setroubleshoot-server setools-console

# Some SELinux tools require this file to be present, ensure it is...
sudo touch /etc/selinux/targeted/contexts/files/file_contexts.local


echo "provision-centos.sh: Done!"


# vim: set expandtab ts=4 sw=4:
