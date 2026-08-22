#!/bin/bash
# Installs Docker + the Compose plugin on a fresh Ubuntu 22.04/24.04 EC2 instance.
# Run once on EACH of the two VMs: bash bootstrap.sh
set -euo pipefail

sudo apt-get update -y
sudo apt-get install -y ca-certificates curl gnupg git

sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update -y
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker "$USER"

echo
echo "Docker installed: $(sudo docker --version)"
echo "Log out and back in (or run 'newgrp docker') for the group membership to take effect,"
echo "then clone the repo and run the appropriate docker-compose.*.yml from cloud/."
