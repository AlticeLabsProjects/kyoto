# -*- mode: ruby -*-
#
# Two VirtualBox VMs are defined below: "debian" (default) and "centos" (optional).
#
# When running "vagrant up" only the "debian" VM is started. To start the "centos"
# VM you have to pass its name as a command argument (eg. "vagrant up centos").
# Since the source tree is mounted from the host, make sure to remove all build
# artifacts (with "make clean") when switching from one environment to the other.
#
# Dependencies: VirtualBox and Vagrant (obviously) with the "vagrant-vbguest" plugin.
# More info: https://www.vagrantup.com/docs/multi-machine/
#

# Required for shared folders support with VirtualBox...
unless Vagrant.has_plugin?("vagrant-vbguest")
    raise 'vagrant-vbguest is not installed: type vagrant plugin install vagrant-vbguest'
end


# Location of the external files used by this script...
vagrant_assets = File.dirname(__FILE__) + "/vagrant"


# Handle future CentOS releases correctly...
class FixGuestAdditionsRH < VagrantVbguest::Installers::RedHat
    def dependencies
        packages = super

        # If there's no "kernel-devel" package matching the running kernel in the
        # default repositories, then the base box we're using doesn't match the
        # latest CentOS release anymore and we have to look for it in the archives...
        if communicate.test('test -f /etc/centos-release && ! yum -q info kernel-devel-`uname -r` &>/dev/null')
            env.ui.warn("[#{vm.name}] Looking for the CentOS 'kernel-devel' package in the release archives...")
            packages.sub!('kernel-devel-`uname -r`', 'http://mirror.centos.org/centos' \
                                                     '/`grep -Po \'\b\d+\.[\d.]+\b\' /etc/centos-release`' \
                                                     '/{os,updates}/`arch`/Packages/kernel-devel-`uname -r`.rpm')
        end

        packages
    end
end


Vagrant.configure(2) do |config|
    config.ssh.keep_alive = true

    # VirtualBox settings common to all VMs defined below...
    config.vm.provider "virtualbox" do |vm, override|
        vm.gui = false

        vm.memory = 512
        vm.cpus = 1

        # Install guest additions automatically...
        override.vbguest.auto_update = true

        # Prevent guest time from drifting uncontrollably on older hosts, even with
        # time synchronization running in the guest VM. If you have a fairly recent
        # machine this probably won't affect you and can be safely commented-out...
        vm.customize ["modifyvm", :id, "--paravirtprovider", "legacy"]

        # Expose the VM to the host instead of forwarding many ports individually
        # for complex projects. The provisioning script will setup Avahi/mDNS to
        # make the guest VM easily accessible through a "*.local" domain...
        override.vm.network "private_network", type: "dhcp"

        # Make the current directory visible (and editable) inside the VM...
        override.vm.synced_folder ".", "/home/vagrant/kyoto"
    end

    # Debian is the default/primary development environment...
    config.vm.define "debian", primary: true do |debian|
        debian.vm.box = "debian/jessie64"
        debian.vm.hostname = "kyoto-dev-debian"

        debian.vm.provider "virtualbox" do |vm, override|
            vm.name = "Kyoto Development (Debian)"
        end

        # The Debian box defaults to using an rsynced folder...
        debian.vm.synced_folder ".", "/vagrant", disabled: true

        # Perform base-system customizations and install project-specific dependencies...
        debian.vm.provision "shell", path: "#{vagrant_assets}/provision-debian.sh",
                                     privileged: false  # ...run as the "vagrant" user.
    end

    # The CentOS environment can be started with "vagrant up centos"...
    config.vm.define "centos", autostart: false do |centos|
        centos.vm.box = "centos/7"
        centos.vm.hostname = "kyoto-dev-centos"

        centos.vm.provider "virtualbox" do |vm, override|
            vm.name = "Kyoto Development (CentOS)"
            override.vbguest.installer = FixGuestAdditionsRH  # ...see fix above.
        end

        # The CentOS box defaults to using an rsynced folder...
        centos.vm.synced_folder ".", "/vagrant", disabled: true

        # Perform base-system customizations and install project-specific dependencies...
        centos.vm.provision "shell", path: "#{vagrant_assets}/provision-centos.sh",
                                     privileged: false  # ...run as the "vagrant" user.
    end
end


# vim: set expandtab ts=4 sw=4 ft=ruby:
