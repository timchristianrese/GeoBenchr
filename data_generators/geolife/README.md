# Geolife data generation

`generate_data.py` generates synthetic user activity points (`geolife_points`) from Geolife stay
points: it samples the input, learns the distributions of locations, times, users and venues, and
draws new points from them in parallel batches.

Settings are at the top of the script:

| Setting | Default | Meaning |
|---|---|---|
| `INPUT_FILE` | `staypoints.tsv` | Geolife stay points (not in the repository) |
| `SAMPLE_SIZE` | 5,000,000 | rows sampled from the input to learn the distributions |
| `TOTAL_SYNTHETIC` | 4,000,000,000 | points to generate |
| `BATCH_SIZE` | 5,000,000 | points per output file |
| `OUTPUT_DIR` | `synthetic_output/` | Parquet output (git-ignored except `dummy_sample.parquet`) |
| `CACHE_DIR` | `cache/` | the sample and the learned distributions, reused on the next run |

```sh
python3 generate_data.py
```
