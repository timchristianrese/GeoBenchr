import os
import json

base_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(base_dir, "..", "data", "processed", "human")
output_dataset = os.path.join(base_dir, "..", "data", "training", "human_sequences.jsonl")
vocab_path = os.path.join(base_dir, "..", "data", "training", "node_vocab.json")

os.makedirs(os.path.dirname(output_dataset), exist_ok=True)

# Containers for all sequences and all unique node IDs
all_sequences = []
node_set = set()

# Read each processed JSON trajectory file
for i, filename in enumerate(os.listdir(input_dir)):
    if filename.endswith(".json"):
        print(f"Lecture fichier {i}: {filename}")
        with open(os.path.join(input_dir, filename)) as f:
            sequence = json.load(f)
            # Keep only sequences that are long enough
            if len(sequence) > 10:
                all_sequences.append(sequence)
                node_set.update(sequence)

# Save all sequences to the JSONL file (1 sequence per line)
with open(output_dataset, "w") as f:
    for seq in all_sequences:
        f.write(json.dumps(seq) + "\n")

# Build a mapping from node ID to index and the reverse
node_list = sorted(list(node_set))
node_to_index = {node_id: i for i, node_id in enumerate(node_list)}
index_to_node = {i: node_id for i, node_id in enumerate(node_list)}

# Save the vocab to JSON
with open(vocab_path, "w") as f:
    json.dump({
        "node_to_index": node_to_index,
        "index_to_node": index_to_node
    }, f)

print(f"Sequences kept: {len(all_sequences)}")
print(f"Unique nodes found: {len(node_list)}")
print(f"Training dataset saved to: {output_dataset}")
print(f"Vocabulary saved to: {vocab_path}")
