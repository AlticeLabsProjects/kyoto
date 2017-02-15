# -*- mode: ruby -*-
#
# Two VirtualBox VMs are defined below: "debian" (default) and "centos" (optional).
#
# When running "vagrant up" only the "debian" VM is started. To start the "centos"
# VM you have to pass its name as a command argument (eg. "vagrant up centos").
#
# The source tree is rsync'ed from the host because (unfortunately) VirtualBox shared
# folders do not support "mmap()" with "MAP_SHARED" and this prevents the test suite
# from running. Use "vagrant rsync" or "vagrant rsync-auto" to keep it synchronized.
#
# More info: https://www.vagrantup.com/docs/multi-machine/
#


# Location of the external files used by this script...
vagrant_assets = File.dirname(__FILE__) + "/vagrant"


Vagrant.configure(2) do |config|
    config.ssh.keep_alive = true

    # VirtualBox settings common to all VMs defined below...
    config.vm.provider "virtualbox" do |vm, override|
        vm.gui = false

        vm.memory = 512
        vm.cpus = 1

        # Prevent guest time from drifting uncontrollably on older hosts, even with
        # time synchronization running in the guest VM. If you have a fairly recent
        # machine this probably won't affect you and can be safely commented-out...
        vm.customize ["modifyvm", :id, "--paravirtprovider", "legacy"]

        # Expose the VM to the host instead of forwarding many ports individually
        # for complex projects. The provisioning script will setup Avahi/mDNS to
        # make the guest VM easily accessible through a "*.local" domain...
        override.vm.network "private_network", type: "dhcp"

        # See note about "mmap()" failures with VirtualBox shared folders above...
        override.vm.synced_folder ".", "/home/vagrant/kyoto", type: "rsync",
                                  rsync__exclude: ".*", rsync__args: ["-a"]  # ...keep build artifacts.
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
        end

        # The CentOS box defaults to using an rsynced folder...
        centos.vm.synced_folder ".", "/vagrant", disabled: true

        # Perform base-system customizations and install project-specific dependencies...
        centos.vm.provision "shell", path: "#{vagrant_assets}/provision-centos.sh",
                                     privileged: false  # ...run as the "vagrant" user.
    end
end


# vim: set expandtab ts=4 sw=4 ft=ruby:
