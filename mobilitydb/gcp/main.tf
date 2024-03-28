# data "terraform_remote_state" "main_state" {
#   backend = "local"

#   config = {
#     path = "../../base/terraform.tfstate"
#   }
# }

resource "google_compute_network" "vpc_network" {
  name = "sut-network"
}

resource "google_compute_firewall" "ssh-rule" {
  name = "ssh-access-and-db-ports"
  network = google_compute_network.vpc_network.name
  allow {
    protocol = "tcp"
    ports = ["22","4445","9132","9133","9995","9996","9997","9998","9999","12234","42424","10001","10002","7000","7001","7199","9042","9160","9142","5432","25432"]
  }
  target_tags = ["sut"]
  source_ranges = ["0.0.0.0/0"]
}

resource "google_compute_instance" "sut" {
  name         = "mobilitydb-under-test"
  machine_type = "n1-standard-1"
  zone         = "europe-west4-c"
  tags         = [ "sut" ]
  boot_disk {
    initialize_params {
      image = "ubuntu-2204-lts"
    }
  }
  network_interface {
    network = google_compute_network.vpc_network.name

    access_config {
      // Ephemeral public IP
    }
  }

  metadata = {
    system-under-test = "mobilitydb"
    ssh-keys = "${var.gcp_ssh_user}:${file(var.gcp_ssh_pub_key_file)}"
  }

  metadata_startup_script = "${file("../scripts/single_start.sh")}"

}