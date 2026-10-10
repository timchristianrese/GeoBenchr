#!/usr/bin/env bash
# Provisions 5 local Debian 12 VMs (via KVM/libvirt + cloud-init) on a clean Ubuntu host,
# matching the topology server/multinode/ansible/inventory/local/hosts.yml expects: geobenchr-sut-5
# as the single "masters" node, geobenchr-sut-1..4 as "workers". Meant for testing the Ansible
# playbooks under server/multinode/ansible against disposable local infrastructure instead of the
# real cluster.
#
# Usage:
#   ./setup_local_vms.sh              # create/start all 5 VMs
#   ./setup_local_vms.sh --force      # destroy + recreate any VM that already exists
#
# Afterwards, run ansible from server/multinode/ansible with the generated inventory:
#   ansible-playbook -i inventory/local/hosts.local.yml playbooks/all.yml -v
#
# Tunables (env vars): VM_VCPUS, VM_MEM_MB, VM_DISK_GB, DEBIAN_SUITE, NET_NAME, SUBNET_PREFIX,
# POOL_DIR, SSH_KEY.

set -euo pipefail

# ---- configuration -------------------------------------------------------------------------
VM_VCPUS="${VM_VCPUS:-2}"
VM_MEM_MB="${VM_MEM_MB:-4096}"
VM_DISK_GB="${VM_DISK_GB:-40}"
DEBIAN_SUITE="${DEBIAN_SUITE:-bookworm}"        # Debian codename, e.g. bookworm (12), trixie (13)
NET_NAME="${NET_NAME:-geobenchr-net}"
SUBNET_PREFIX="${SUBNET_PREFIX:-10.122.0}"      # matches the convention already used in hosts.yml
POOL_DIR="${POOL_DIR:-/var/lib/libvirt/images/geobenchr}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_ed25519}"
FORCE=0

for arg in "$@"; do
  case "$arg" in
    --force) FORCE=1 ;;
    -h|--help) grep '^#' "$0" | sed 's/^#//'; exit 0 ;;
    *) echo "Unknown argument: $arg" >&2; exit 1 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ANSIBLE_DIR="$(cd "$SCRIPT_DIR/../ansible" && pwd)"
WORK_DIR="$SCRIPT_DIR/.work"
mkdir -p "$WORK_DIR"

IMAGE_DIR_URL="https://cloud.debian.org/images/cloud/${DEBIAN_SUITE}/latest/"
IMAGE_FILENAME="$(curl -fsSL "$IMAGE_DIR_URL" | grep -oE 'href="[A-Za-z0-9.-]+-genericcloud-amd64\.qcow2"' | head -1 | sed -E 's/^href="//; s/"$//')"
if [ -z "$IMAGE_FILENAME" ]; then
  echo "ERROR: could not find a genericcloud-amd64.qcow2 image for suite '${DEBIAN_SUITE}' under ${IMAGE_DIR_URL}" >&2
  exit 1
fi
IMAGE_URL="${IMAGE_DIR_URL}${IMAGE_FILENAME}"
BASE_IMAGE="$WORK_DIR/${IMAGE_FILENAME}"

# node name -> [group, num_id, host-octet, mac suffix]
NODES=(geobenchr-sut-1 geobenchr-sut-2 geobenchr-sut-3 geobenchr-sut-4 geobenchr-sut-5)
declare -A NODE_GROUP=( [geobenchr-sut-1]=workers [geobenchr-sut-2]=workers [geobenchr-sut-3]=workers [geobenchr-sut-4]=workers [geobenchr-sut-5]=masters )
declare -A NODE_NUMID=( [geobenchr-sut-1]=1 [geobenchr-sut-2]=2 [geobenchr-sut-3]=3 [geobenchr-sut-4]=4 [geobenchr-sut-5]=5 )
declare -A NODE_OCTET=( [geobenchr-sut-1]=101 [geobenchr-sut-2]=102 [geobenchr-sut-3]=103 [geobenchr-sut-4]=104 [geobenchr-sut-5]=105 )
declare -A NODE_MAC=(   [geobenchr-sut-1]="52:54:00:9e:73:01" [geobenchr-sut-2]="52:54:00:9e:73:02" [geobenchr-sut-3]="52:54:00:9e:73:03" [geobenchr-sut-4]="52:54:00:9e:73:04" [geobenchr-sut-5]="52:54:00:9e:73:05" )

log() { echo -e "\n>>> $*"; }

# ---- 1. host prerequisites ------------------------------------------------------------------
log "Installing KVM/libvirt prerequisites"
sudo apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
  qemu-system-x86 qemu-utils libvirt-daemon-system libvirt-clients virtinst \
  cloud-image-utils genisoimage wget openssh-client cloud-guest-utils gdisk >/dev/null

sudo modprobe nbd max_part=16

sudo usermod -aG libvirt,kvm "$USER" || true

KVM_OK=0
if [ -e /dev/kvm ]; then
  KVM_OK=1
else
  echo "WARNING: /dev/kvm not present (no hardware virtualization, or running nested without it enabled)." >&2
  echo "         VMs will still be created but will use slow software emulation." >&2
fi

sudo systemctl enable --now libvirtd >/dev/null

# ---- 2. ssh key -------------------------------------------------------------------------------
if [ ! -f "$SSH_KEY" ]; then
  log "Generating SSH key at $SSH_KEY (used to reach the VMs as user 'debian')"
  ssh-keygen -t ed25519 -N "" -f "$SSH_KEY" -C "geobenchr-local-vms" >/dev/null
fi
SSH_PUBKEY_CONTENT="$(cat "${SSH_KEY}.pub")"

# ---- 3. base cloud image -----------------------------------------------------------------------
if [ ! -f "$BASE_IMAGE" ]; then
  log "Downloading Debian ${DEBIAN_SUITE} generic cloud image"
  wget -q --show-progress -O "$BASE_IMAGE.tmp" "$IMAGE_URL"
  wget -q -O "$WORK_DIR/SHA512SUMS" "https://cloud.debian.org/images/cloud/${DEBIAN_SUITE}/latest/SHA512SUMS"
  EXPECTED="$(grep "${IMAGE_FILENAME}\$" "$WORK_DIR/SHA512SUMS" | awk '{print $1}')"
  ACTUAL="$(sha512sum "$BASE_IMAGE.tmp" | awk '{print $1}')"
  if [ "$EXPECTED" != "$ACTUAL" ]; then
    echo "ERROR: checksum mismatch on downloaded Debian cloud image" >&2
    rm -f "$BASE_IMAGE.tmp"
    exit 1
  fi
  mv "$BASE_IMAGE.tmp" "$BASE_IMAGE"
else
  log "Reusing cached base image at $BASE_IMAGE"
fi

sudo mkdir -p "$POOL_DIR"

# qemu (running as the unprivileged libvirt-qemu user) needs to read this file as a backing file
# for every VM's disk. $WORK_DIR lives under $HOME, which isn't traversable by other users, so
# the backing file actually used has to live under the pool dir instead (owned by root, matching
# the default /var/lib/libvirt/images permissions, world-readable).
POOL_BASE_IMAGE="$POOL_DIR/${IMAGE_FILENAME}"
if [ ! -f "$POOL_BASE_IMAGE" ]; then
  sudo install -m 0644 "$BASE_IMAGE" "$POOL_BASE_IMAGE"
fi

# ---- 4. libvirt network -------------------------------------------------------------------------
if ! sudo virsh net-info "$NET_NAME" >/dev/null 2>&1; then
  log "Creating libvirt network '$NET_NAME' (${SUBNET_PREFIX}.0/24)"
  NET_XML="$WORK_DIR/network.xml"
  {
    echo "<network>"
    echo "  <name>${NET_NAME}</name>"
    echo "  <bridge name='virbr-gbr' stp='on' delay='0'/>"
    echo "  <forward mode='nat'/>"
    echo "  <ip address='${SUBNET_PREFIX}.1' netmask='255.255.255.0'>"
    echo "    <dhcp>"
    echo "      <range start='${SUBNET_PREFIX}.10' end='${SUBNET_PREFIX}.100'/>"
    for node in "${NODES[@]}"; do
      echo "      <host mac='${NODE_MAC[$node]}' name='${node}' ip='${SUBNET_PREFIX}.${NODE_OCTET[$node]}'/>"
    done
    echo "    </dhcp>"
    echo "  </ip>"
    echo "</network>"
  } > "$NET_XML"
  sudo virsh net-define "$NET_XML"
  sudo virsh net-autostart "$NET_NAME"
  sudo virsh net-start "$NET_NAME"
else
  log "libvirt network '$NET_NAME' already exists, reusing it"
  if [ "$(sudo virsh net-info "$NET_NAME" | awk '/^Active/{print $2}')" != "yes" ]; then
    sudo virsh net-start "$NET_NAME"
  fi
fi

# ---- 5. per-node VM creation ---------------------------------------------------------------------
for node in "${NODES[@]}"; do
  log "Provisioning $node (${SUBNET_PREFIX}.${NODE_OCTET[$node]})"

  if sudo virsh dominfo "$node" >/dev/null 2>&1; then
    if [ "$FORCE" -eq 1 ]; then
      echo "  --force: destroying existing domain $node"
      sudo virsh destroy "$node" >/dev/null 2>&1 || true
      sudo virsh undefine "$node" --remove-all-storage --nvram >/dev/null 2>&1 || true
    else
      echo "  domain $node already exists, skipping (use --force to recreate)"
      continue
    fi
  fi

  NODE_DIR="$WORK_DIR/$node"
  mkdir -p "$NODE_DIR"

  cat > "$NODE_DIR/user-data" <<EOF
#cloud-config
hostname: ${node}
fqdn: ${node}.geobenchr.local
manage_etc_hosts: false
ssh_pwauth: false
users:
  - default
ssh_authorized_keys:
  - ${SSH_PUBKEY_CONTENT}
package_update: true
packages:
  - python3
EOF

  cat > "$NODE_DIR/meta-data" <<EOF
instance-id: ${node}
local-hostname: ${node}
EOF

  cloud-localds "$NODE_DIR/seed.iso" "$NODE_DIR/user-data" "$NODE_DIR/meta-data"

  DISK_PATH="$POOL_DIR/${node}.qcow2"
  SEED_PATH="$POOL_DIR/${node}-seed.iso"
  sudo qemu-img create -f qcow2 -F qcow2 -b "$POOL_BASE_IMAGE" "$DISK_PATH" "${VM_DISK_GB}G" >/dev/null
  sudo cp "$NODE_DIR/seed.iso" "$SEED_PATH"

  # FIX: cloud-init never actually runs on these VMs (confirmed via the offline journal -- zero
  # cloud-init log lines on a genuinely fresh single boot), and separately sshd never starts either
  # ("no hostkeys available -- exiting", looping forever): with /etc/machine-id empty, systemd
  # under KVM derives a *transient* machine ID from the DMI/SMBIOS UUID instead of doing a real
  # first-boot generation, which never satisfies `ConditionFirstBoot=yes` -- so
  # sshd-keygen.service (gated on that condition) permanently skips, on every boot, forever, not
  # just the first one. Since this environment can't be relied on to run cloud-init at all,
  # provision hostname/network/SSH access/host keys directly on the disk before first boot instead
  # of via the cloud-init seed ISO (still attached above -- harmless if cloud-init ever does start
  # working here, just redundant).
  NBD_DEV="/dev/nbd8"
  sudo qemu-nbd --connect="$NBD_DEV" "$DISK_PATH"
  for i in $(seq 1 20); do [ -e "${NBD_DEV}p1" ] && break; sleep 0.5; done
  sudo growpart "$NBD_DEV" 1 || true
  sudo e2fsck -fy "${NBD_DEV}p1" >/dev/null 2>&1 || true
  sudo resize2fs "${NBD_DEV}p1" >/dev/null

  MNT_DIR="$WORK_DIR/mnt-$node"
  sudo mkdir -p "$MNT_DIR"
  sudo mount "${NBD_DEV}p1" "$MNT_DIR"

  echo "$node" | sudo tee "$MNT_DIR/etc/hostname" >/dev/null

  sudo mkdir -p "$MNT_DIR/etc/systemd/network"
  printf '[Match]\nName=en* eth*\n\n[Network]\nDHCP=yes\n' | sudo tee "$MNT_DIR/etc/systemd/network/10-dhcp.network" >/dev/null

  # The base genericcloud image ships only "root" -- the "debian" user this project's Ansible
  # inventory (ansible_user: debian) and SSH access both assume is normally created by cloud-init's
  # "users: [default]" directive, which never runs here (see above).
  sudo chroot "$MNT_DIR" useradd -m -s /bin/bash debian 2>/dev/null || true
  echo "debian ALL=(ALL) NOPASSWD:ALL" | sudo tee "$MNT_DIR/etc/sudoers.d/90-debian" >/dev/null
  sudo chmod 440 "$MNT_DIR/etc/sudoers.d/90-debian"
  sudo mkdir -p "$MNT_DIR/home/debian/.ssh"
  echo "$SSH_PUBKEY_CONTENT" | sudo tee "$MNT_DIR/home/debian/.ssh/authorized_keys" >/dev/null
  sudo chmod 700 "$MNT_DIR/home/debian/.ssh"
  sudo chmod 600 "$MNT_DIR/home/debian/.ssh/authorized_keys"
  sudo chroot "$MNT_DIR" chown -R debian:debian /home/debian

  sudo mount --bind /dev "$MNT_DIR/dev"
  sudo chroot "$MNT_DIR" ssh-keygen -A >/dev/null 2>&1
  sudo umount "$MNT_DIR/dev"

  sudo umount "$MNT_DIR"
  sudo qemu-nbd --disconnect "$NBD_DEV" >/dev/null

  VIRT_TYPE_ARGS=(--virt-type qemu)
  CPU_ARGS=(--cpu qemu64)
  if [ "$KVM_OK" -eq 1 ]; then
    VIRT_TYPE_ARGS=(--virt-type kvm)
    CPU_ARGS=(--cpu host-passthrough)
  fi

  # Debian's genericcloud images ship a hybrid GPT (BIOS boot partition + ESP) and boot fine
  # under legacy SeaBIOS on bookworm, but trixie's image hits a GRUB/gfxterm init failure under
  # legacy BIOS on a headless (no video device) VM -- GRUB repeatedly clears the screen and
  # reprints "Booting `Debian GNU/Linux'" without ever reaching the kernel, resetting in a loop
  # roughly once a second, forever. `--boot uefi` (OVMF, auto-selected via libvirt's firmware
  # descriptors) boots the same image cleanly and works for both suites, so use it unconditionally.
  sudo virt-install \
    --name "$node" \
    --memory "$VM_MEM_MB" \
    --vcpus "$VM_VCPUS" \
    "${VIRT_TYPE_ARGS[@]}" \
    "${CPU_ARGS[@]}" \
    --osinfo detect=on,require=off \
    --boot uefi \
    --disk path="$DISK_PATH",format=qcow2,bus=virtio \
    --disk path="$SEED_PATH",device=cdrom \
    --network network="$NET_NAME",model=virtio,mac="${NODE_MAC[$node]}" \
    --graphics none \
    --console pty,target_type=serial \
    --import \
    --noautoconsole >/dev/null
done

# ---- 6. wait for SSH to come up -------------------------------------------------------------------
log "Waiting for all nodes to become reachable over SSH"
SSH_WAIT_FAILED=0
for node in "${NODES[@]}"; do
  ip="${SUBNET_PREFIX}.${NODE_OCTET[$node]}"
  printf "  %s (%s) " "$node" "$ip"
  node_ready=0
  for i in $(seq 1 60); do
    if ssh -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
         -o ConnectTimeout=3 "debian@${ip}" true 2>/dev/null; then
      echo "ready"
      node_ready=1
      break
    fi
    if [ "$i" -eq 60 ]; then
      echo "TIMED OUT"
    fi
    sleep 5
  done
  [ "$node_ready" -eq 1 ] || SSH_WAIT_FAILED=1
done

# FIX: this used to fall through to writing the inventory and printing "Done." / exit 0 even when
# every node timed out above -- silently handing back a working-looking inventory for VMs nothing
# can actually reach, discovered when a trixie GRUB/gfxterm boot loop (see --boot uefi above) left
# all 5 nodes unreachable but the script still reported success.
if [ "$SSH_WAIT_FAILED" -eq 1 ]; then
  echo "ERROR: one or more nodes never became reachable over SSH -- not writing the inventory." >&2
  echo "       Check console output (virsh console <node>, or read /dev/pts/N per 'virsh dumpxml <node> | grep console') to diagnose." >&2
  exit 1
fi

# ---- 7. write the ansible inventory ------------------------------------------------------------
INVENTORY_PATH="$ANSIBLE_DIR/inventory/local/hosts.local.yml"
log "Writing inventory to $INVENTORY_PATH"

{
  echo "all:"
  echo "  children:"
  echo "    masters:"
  echo "      hosts:"
  for node in "${NODES[@]}"; do
    [ "${NODE_GROUP[$node]}" = masters ] || continue
    echo "        ${node}:"
    echo "          ansible_host: ${SUBNET_PREFIX}.${NODE_OCTET[$node]}"
    echo "          ip: ${SUBNET_PREFIX}.${NODE_OCTET[$node]}"
    echo "          hostname: ${node}"
    echo "          num_id: ${NODE_NUMID[$node]}"
  done
  echo "    workers:"
  echo "      hosts:"
  for node in "${NODES[@]}"; do
    [ "${NODE_GROUP[$node]}" = workers ] || continue
    echo "        ${node}:"
    echo "          ansible_host: ${SUBNET_PREFIX}.${NODE_OCTET[$node]}"
    echo "          ip: ${SUBNET_PREFIX}.${NODE_OCTET[$node]}"
    echo "          hostname: ${node}"
    echo "          num_id: ${NODE_NUMID[$node]}"
  done
  echo "    clickhouse:"
  echo "      children:"
  echo "        masters:"
  echo "        workers:"
} > "$INVENTORY_PATH"

log "Done."
echo "5 VMs are up. SSH key: $SSH_KEY"
echo "Test connectivity with:"
echo "  cd $ANSIBLE_DIR && ansible -i inventory/local/hosts.local.yml all -m ping --private-key=$SSH_KEY"
echo "Run the playbooks with:"
echo "  cd $ANSIBLE_DIR && ansible-playbook -i inventory/local/hosts.local.yml playbooks/all.yml -v --private-key=$SSH_KEY"
echo "(or add 'ansible_ssh_private_key_file: $SSH_KEY' to inventory/local/group_vars/all/all.yml to avoid passing --private-key every time)"
