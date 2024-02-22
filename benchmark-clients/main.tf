resource "google_compute_instance" "sut" {
  count        = var.clients
  name         = "benchmarking-client${count.index}"
  machine_type = "n2-standard-8"
  zone         = "europe-west2-c"
  tags         = [ "sut" ]
  boot_disk {
    initialize_params {
      image = "ubuntu-2204-lts"
      size  = "200"
      type  = "pd-ssd"
    }
  }

  // Local SSD disk
  scratch_disk {
    interface = "NVME"
  }

  network_interface {
    network = google_compute_network.vpc_network.name

    access_config {
      // Ephemeral public IP
    }
  }

  metadata = {
    system-under-test = "postgis"
    ssh-keys = "${var.gcp_ssh_user}:${file(var.gcp_ssh_pub_key_file)}"
  }

  metadata_startup_script = "${file("../scripts/single_start.sh")}"

}

resource "google_compute_firewall" "ssh-rule" {
  name = "ssh-access"
  network = google_compute_network.vpc_network.name
  allow {
    protocol = "tcp"
    ports = ["22"]
  }
  target_tags = ["sut"]
  source_ranges = ["0.0.0.0/0"]
}

resource "google_compute_network" "vpc_network" {
  name = "bench-network"
}