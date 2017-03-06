#!/bin/bash -e
#
# Provision Debian VM (vagrant shell provisioner).
#


if [ "$(id -u)" != "$(id -u vagrant)" ]; then
    echo "The provisioning script must be run as the \"vagrant\" user!" >&2
    exit 1
fi


echo "provision-debian.sh: Customizing the base system..."

sudo DEBIAN_FRONTEND=noninteractive apt-get -qq update

sudo DEBIAN_FRONTEND=noninteractive apt-get -qq -y install \
    avahi-daemon mlocate lsof iotop htop \
    ntpdate pv tree vim ltrace strace

# This is just a matter of preference...
sudo DEBIAN_FRONTEND=noninteractive apt-get -qq -y install netcat-openbsd
sudo DEBIAN_FRONTEND=noninteractive apt-get -qq -y purge netcat-traditional


# Set a local timezone (the default for Debian boxes is GMT)...
sudo timedatectl set-timezone "Europe/Lisbon"

sudo systemctl -q enable systemd-timesyncd
sudo systemctl start systemd-timesyncd

# This gives us an easly reachable ".local" name for the VM...
sudo systemctl -q enable avahi-daemon 2>/dev/null
sudo systemctl start avahi-daemon

# Prevent locale from being forwarded from the host, causing issues...
if sudo grep -q '^AcceptEnv\s.*LC_' /etc/ssh/sshd_config; then
    sudo sed -i 's/^\(AcceptEnv\s.*LC_\)/#\1/' /etc/ssh/sshd_config
    sudo systemctl restart ssh
fi

# Generate the initial "locate" DB...
if sudo test -x /etc/cron.daily/mlocate; then
    sudo /etc/cron.daily/mlocate
fi

# Remove the spurious "you have mail" message on login...
if [ -s "/var/spool/mail/$USER" ]; then
    > "/var/spool/mail/$USER"
fi


echo "provision-debian.sh: Running project-specific actions..."

# Install extra packages needed for the project...
sudo DEBIAN_FRONTEND=noninteractive apt-get -qq -y install \
    build-essential \
    zlib1g-dev liblzo2-dev liblua5.1-0-dev


echo "provision-debian.sh: Done!"


# vim: set expandtab ts=4 sw=4:
