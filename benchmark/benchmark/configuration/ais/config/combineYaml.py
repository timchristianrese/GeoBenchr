import yaml
from yaml.representer import SafeRepresenter

# File paths
mobility_file = "mobilityDBBenchConf.yaml"
postgis_file = "postgisSQLBenchConf.yaml"
sedona_file = "sedonaSQLBenchConf.yaml"
out_file = "combinedBenchConf.yaml"

# Custom string presenter to force block style where needed
def str_presenter(dumper, data):
    if '\n' in data:
        return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
    return dumper.represent_scalar('tag:yaml.org,2002:str', data)

yaml.add_representer(str, str_presenter)

# Load YAML files
def load_yaml(file_path):
    with open(file_path, 'r') as f:
        return yaml.safe_load(f)

mobility_data = load_yaml(mobility_file)
postgis_data = load_yaml(postgis_file)
sedona_data = load_yaml(sedona_file)

# Extract sections
benchmark_section = mobility_data.get("benchmark", {})
mobility_queries = mobility_data.get("queryConfigs", [])
postgis_queries = postgis_data.get("queryConfigs", [])
sedona_queries = sedona_data.get("queryConfigs", [])

# Index by name for lookup
postgis_map = {q['name']: q for q in postgis_queries}
sedona_map = {q['name']: q for q in sedona_queries}

combined_queries = []

for entry in mobility_queries:
    print(entry['name'])
    name = entry['name']
    combined_entry = {
        'name': name,
        'use': entry.get('use', False),
        'type': entry.get('type', 'temporalbench'),
        'mobilitydb': entry.get('sql', ''),
        'postgis': postgis_map.get(name, {}).get('sql', ''),
        'sedona': sedona_map.get(name, {}).get('sql', ''),
        'repetition': entry.get('repetition', 1),
        'parameters': entry.get('parameters', [])
    }
    combined_queries.append(combined_entry)

# Combine everything under proper structure
final_output = {
    'benchmark': benchmark_section,
    'queryConfigs': combined_queries
}

# Save to output file
with open(out_file, 'w') as f:
    yaml.dump(final_output, f, sort_keys=False, allow_unicode=True)

print(f"Combined YAML written to {out_file}")