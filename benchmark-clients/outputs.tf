output "gcp_global_ip" {
  value = ["${google_compute_instance[*].sut.network_interface.0.access_config.0.nat_ip}"]
}