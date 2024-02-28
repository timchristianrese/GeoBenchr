resource "google_storage_bucket" "simra_data" {
  name          = "simra-data"
  location      = "EU"
  force_destroy = true

  uniform_bucket_level_access = false
  lifecycle {
      prevent_destroy = true
  }
}

resource "google_storage_bucket_iam_member" "member" {
  bucket = google_storage_bucket.simra_data.name
  role   = "roles/storage.objectViewer"
  member = "allUsers"
}


resource "google_storage_bucket_object" "simra" {
    source       = "../data/Berlin_102022_07_2023.zip"
    content_type = "application/zip"

    # Append to the MD5 checksum of the files's content
    # to force the zip to be updated as soon as a change occurs
    name         = "simra-data.zip"
    bucket       = google_storage_bucket.simra_data.name
    lifecycle {
        prevent_destroy = true
    }
}

resource "google_compute_firewall" "ssh-rule" {
  name = "ssh-access-and-db-ports"
  network = google_compute_network.vpc_network.name
  allow {
    protocol = "tcp"
    ports = ["22","4445","9132","9133","9995","9996","9997","9998","9999","12234","42424","10001","10002","7000","7001","7199","9042","9160","9142","5432"]
  }
  target_tags = ["sut"]
  source_ranges = ["0.0.0.0/0"]
}

resource "google_compute_network" "vpc_network" {
  name = "sut-network"
}

output "vpcname" {
  value = google_compute_network.vpc_network.name
}