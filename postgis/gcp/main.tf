data "terraform_remote_state" "main_state" {
  backend = "local"

  config = {
    path = "../../base/terraform.tfstate"
  }
}

resource "google_compute_instance" "sut" {
  name         = "postgis-under-test"
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
    network = data.terraform_remote_state.main_state.outputs.vpcname

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