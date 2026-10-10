# Local 5-node VM setup

Spins up 5 local Debian 12 VMs on a clean Ubuntu host (via KVM/libvirt + cloud-init) so the
Ansible playbooks in `server/multinode/ansible` can be tested end-to-end without touching the real
cluster. Topology matches what `ansible/inventory/local/hosts.yml` expects: `geobenchr-sut-5` is the
`masters` node (Citus coordinator / DuckDB host), `geobenchr-sut-1..4` are `workers`.

Requires a Linux host (tested against a fresh Ubuntu install); for hardware virtualization it also
needs `/dev/kvm` to be available (bare metal, or a VM/cloud instance with nested virtualization
enabled) — the script still works without it, just much slower, via QEMU software emulation.

## Usage

```
./setup_local_vms.sh          # create + start all 5 VMs, wait for SSH, write the inventory
./setup_local_vms.sh --force  # destroy and recreate any VM that already exists
```

This will:
1. Install `qemu`/`libvirt`/`virtinst`/`cloud-image-utils` via `apt`.
2. Download the official Debian 12 "genericcloud" qcow2 image (checksum-verified) and cache it
   under `.work/`.
3. Generate an SSH keypair at `~/.ssh/id_ed25519` if one doesn't already exist.
4. Create a NAT'd libvirt network (`10.122.0.0/24` by default) with static DHCP reservations for
   each node.
5. Create the 5 VMs from the cached base image, seeded via cloud-init with hostname, the `debian`
   user (Debian's cloud image default user, already passwordless-sudo), and your SSH public key.
6. Wait until all 5 respond over SSH.
7. Write `../ansible/inventory/local/hosts.local.yml` (gitignored, doesn't touch the checked-in
   `inventory/local/hosts.yml`) in the same shape, so it picks up the same
   `inventory/local/group_vars/all/all.yml`.

Then, from `server/multinode/ansible`:

```
ansible -i inventory/local/hosts.local.yml all -m ping --private-key=~/.ssh/id_ed25519
ansible-playbook -i inventory/local/hosts.local.yml playbooks/all.yml -v --private-key=~/.ssh/id_ed25519
```

(Or add `ansible_ssh_private_key_file: ~/.ssh/id_ed25519` to `inventory/local/group_vars/all/all.yml`
locally to skip passing `--private-key` every time — don't commit that change.)

Note this is entirely separate from `inventory/nst-cluster/`, the real partner-owned 5-node
SpaceTime cluster documented in `../ansible/README.md` -- these disposable local VMs are only for
testing the Ansible playbooks themselves before running them against that production hardware.

## Teardown

```
./destroy_local_vms.sh                    # remove VMs, disks, network, cached image
./destroy_local_vms.sh --keep-network      # leave the libvirt network defined
./destroy_local_vms.sh --keep-image-cache  # keep the downloaded base image for next time
```

## Tunables

Set as environment variables before running `setup_local_vms.sh`:

| Variable        | Default                              | Meaning                              |
|-----------------|---------------------------------------|---------------------------------------|
| `VM_VCPUS`      | `2`                                    | vCPUs per VM                          |
| `VM_MEM_MB`     | `4096`                                 | RAM per VM in MB                      |
| `VM_DISK_GB`    | `40`                                   | Disk size per VM in GB                |
| `DEBIAN_SUITE`  | `bookworm`                             | Debian codename (e.g. `bookworm`=12, `trixie`=13) |
| `NET_NAME`      | `geobenchr-net`                        | libvirt network name                  |
| `SUBNET_PREFIX` | `10.122.0`                             | First 3 octets of the private subnet  |
| `POOL_DIR`      | `/var/lib/libvirt/images/geobenchr`    | Where VM disks/seed ISOs are stored   |
| `SSH_KEY`       | `~/.ssh/id_ed25519`                    | Key installed into the VMs            |

Defaults total 20 GB RAM / 10 vCPUs / 200 GB disk across all 5 VMs — the real SUT stack
(Postgres/Citus/MobilityDB, ClickHouse, Hadoop/Spark/Sedona, DuckDB) is heavy, so lower them if
your host doesn't have the headroom, but very small VMs (e.g. Hadoop/Spark) may struggle to start.
