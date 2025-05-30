# model2/generate/map_display.py

import folium
from interface import TrajectoryGenerator
import os

#Initialise le générateur
generator = TrajectoryGenerator()

#Paramètres
N_TRAJETS = 15
output_file = "./output/generated_map.html"
os.makedirs(os.path.dirname(output_file), exist_ok=True)

# Initialise la carte
m = folium.Map(location=[52.52, 13.405], zoom_start=13)

nb_success = 0
for i in range(N_TRAJETS):
    print(f"Génération du trajet {i + 1}/{N_TRAJETS}")
    try:
        node_ids = generator.generate()
        print(f"   ➤ {len(node_ids)} nœuds générés.")
        coords = generator.node_ids_to_coordinates(node_ids)

        if len(coords) > 1:
            folium.PolyLine(coords, color="blue", weight=2, opacity=0.7,
                            tooltip=f"Trajet {i + 1}").add_to(m)
            folium.Marker(coords[0], tooltip="Départ",
                          icon=folium.Icon(color="green")).add_to(m)
            #folium.Marker(coords[-1], tooltip="Arrivée",
                          #icon=folium.Icon(color="red")).add_to(m)
            nb_success += 1
        else:
            print("⚠️ Trajet trop court, ignoré.")
    except Exception as e:
        print(f"Erreur lors de la génération du trajet {i + 1} : {e}")

#Sauvegarde
if nb_success == 0:
    print("Aucun trajet valide généré. Vérifie ton modèle ou graphe.")
else:
    m.save(output_file)
    print(f"{nb_success} trajets affichés sur la carte.")
    print(f"Carte sauvegardée : {output_file}")
