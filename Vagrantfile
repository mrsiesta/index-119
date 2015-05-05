VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|

  config.vm.define "indexer" do |indexer|
    indexer.vm.box = "ubuntu1404"
    indexer.vm.hostname = "indexer"
    indexer.vm.network "private_network", ip: "192.168.60.10"
    indexer.vm.box_url = "https://oss-binaries.phusionpassenger.com/vagrant/boxes/latest/ubuntu-14.04-amd64-vbox.box"
    indexer.vm.provision "ansible" do |ansible|
      ansible.playbook = "./ansible/indexer.yaml"
      ansible.sudo = true
      ansible.extra_vars = { ansible_ssh_user: 'vagrant' }
    end
    indexer.vm.provider "virtualbox" do |v|
      v.customize ["modifyvm", :id, "--memory", 8192]
      v.customize ["modifyvm", :id, "--cpus", 4]
    end
  end

end
