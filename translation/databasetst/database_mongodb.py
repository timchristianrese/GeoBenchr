from pymongo import MongoClient
from datetime import datetime, UTC

client = MongoClient("mongodb://localhost:27017/")
db = client["my_base"]

db.drop_collection("utilisateurs")
db.drop_collection("commandes")
db.drop_collection("voitures")
db.drop_collection("districts")     
db.drop_collection("points_interet")
db.drop_collection("depots")        
db.drop_collection("routes")       

utilisateurs = [
    { "nom": "Dupont",  "prenom": "Alice",   "age": 30, "pays": "Allemagne", "date_inscription": datetime(2024, 6, 1, 9, 0, 0, tzinfo=UTC) },
    { "nom": "Dupont",  "prenom": "Jean",    "age": 28, "pays": "Allemagne", "date_inscription": datetime(2024, 6, 2, 10, 15, 0, tzinfo=UTC) },
    { "nom": "Dupont",  "prenom": "Bernard", "age": 45, "pays": "France",    "date_inscription": datetime(2024, 6, 3, 11, 30, 0, tzinfo=UTC) },
    { "nom": "Ratel",   "prenom": "Romain",  "age": 32, "pays": "France",    "date_inscription": datetime(2024, 6, 4, 12, 45, 0, tzinfo=UTC) },
    { "nom": "Martin",  "prenom": "Arthur",  "age": 22, "pays": "Allemagne", "date_inscription": datetime(2024, 6, 5, 14, 0, 0, tzinfo=UTC) },
    { "nom": "Martin",  "prenom": "Bruno",   "age": 42, "pays": "France",    "date_inscription": datetime(2024, 6, 6, 15, 20, 0, tzinfo=UTC) },
    { "nom": "Lefevre", "prenom": "Caroline","age": 27, "pays": "France",    "date_inscription": datetime(2024, 6, 7, 16, 35, 0, tzinfo=UTC) },
    { "nom": "Nguyen",  "prenom": "David",   "age": 35, "pays": "France",    "date_inscription": datetime(2024, 6, 8, 17, 50, 0, tzinfo=UTC) },
    { "nom": "Bernard", "prenom": "Elodie",  "age": 22, "pays": "France",    "date_inscription": datetime(2024, 6, 9, 19, 5, 0, tzinfo=UTC) },
]
res_users = db.utilisateurs.insert_many(utilisateurs)

ids_by_user = {f"{doc['nom']}|{doc['prenom']}": _id for doc, _id in zip(utilisateurs, res_users.inserted_ids)}

print(f"{db.utilisateurs.count_documents({})} documents insérés dans my_base.utilisateurs")

voitures = [
    { "owner_id": ids_by_user["Dupont|Alice"],   "marque": "Volkswagen", "modele": "Golf",     "annee": 2018 },
    { "owner_id": ids_by_user["Dupont|Jean"],    "marque": "BMW",        "modele": "320d",     "annee": 2016 },
    { "owner_id": ids_by_user["Dupont|Bernard"], "marque": "Renault",    "modele": "Clio",     "annee": 2012 },
    { "owner_id": ids_by_user["Ratel|Romain"],   "marque": "Peugeot",    "modele": "308",      "annee": 2020 },
    { "owner_id": ids_by_user["Martin|Bruno"],   "marque": "Tesla",      "modele": "Model 3",  "annee": 2021 },
    { "owner_id": ids_by_user["Lefevre|Caroline"], "marque": "Fiat",     "modele": "500",      "annee": 2015 },
    { "owner_id": ids_by_user["Nguyen|David"],   "marque": "Audi",       "modele": "A3",       "annee": 2017 },
]
res_cars = db.voitures.insert_many(voitures)

voiture_id_by_owner = {car_doc["owner_id"]: car_id for car_doc, car_id in zip(voitures, res_cars.inserted_ids)}
print(f"{db.voitures.count_documents({})} documents insérés dans my_base.voitures")

now = datetime(2024, 6, 1, 9, 0, 0, tzinfo=UTC)
commandes = [
    { "utilisateur_id": ids_by_user["Dupont|Alice"],  "voiture_id": voiture_id_by_owner[ids_by_user["Dupont|Alice"]],  "montant": 120.50, "statut": "payee",    "pays_livraison": "Allemagne", "date_commande": datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC) },
    { "utilisateur_id": ids_by_user["Dupont|Alice"],  "voiture_id": voiture_id_by_owner[ids_by_user["Dupont|Alice"]],  "montant": 89.00,  "statut": "annulee", "pays_livraison": "Allemagne", "date_commande": datetime(2024, 6, 14, 12, 0, 0, tzinfo=UTC) },

    { "utilisateur_id": ids_by_user["Dupont|Jean"],   "voiture_id": voiture_id_by_owner[ids_by_user["Dupont|Jean"]],   "montant": 230.00, "statut": "payee",   "pays_livraison": "France",    "date_commande": datetime(2024, 5, 26, 12, 0, 0, tzinfo=UTC) },

    { "utilisateur_id": ids_by_user["Dupont|Bernard"],"voiture_id": voiture_id_by_owner[ids_by_user["Dupont|Bernard"]],"montant": 15.00,  "statut": "annulee","pays_livraison": "France",    "date_commande": datetime(2024, 6, 12, 12, 0, 0, tzinfo=UTC) },

    { "utilisateur_id": ids_by_user["Ratel|Romain"],  "voiture_id": voiture_id_by_owner[ids_by_user["Ratel|Romain"]],  "montant": 499.00, "statut": "payee",   "pays_livraison": "France",    "date_commande": datetime(2024, 6, 10, 12, 0, 0, tzinfo=UTC) },

    { "utilisateur_id": ids_by_user["Martin|Bruno"],  "voiture_id": voiture_id_by_owner[ids_by_user["Martin|Bruno"]],  "montant": 79.00,  "statut": "payee",   "pays_livraison": "France",    "date_commande": datetime(2024, 6, 13, 12, 0, 0, tzinfo=UTC) },

    { "utilisateur_id": ids_by_user["Lefevre|Caroline"], "voiture_id": voiture_id_by_owner[ids_by_user["Lefevre|Caroline"]], "montant": 35.50, "statut": "expediee", "pays_livraison": "France", "date_commande": datetime(2024, 6, 7, 12, 0, 0, tzinfo=UTC) },
    { "utilisateur_id": ids_by_user["Lefevre|Caroline"], "voiture_id": voiture_id_by_owner[ids_by_user["Lefevre|Caroline"]], "montant": 12.90, "statut": "annulee",  "pays_livraison": "France", "date_commande": datetime(2024, 6, 11, 12, 0, 0, tzinfo=UTC) },

    { "utilisateur_id": ids_by_user["Nguyen|David"],  "voiture_id": voiture_id_by_owner[ids_by_user["Nguyen|David"]],  "montant": 999.99, "statut": "payee",   "pays_livraison": "France",    "date_commande": datetime(2024, 6, 3, 12, 0, 0, tzinfo=UTC) },

]
db.commandes.insert_many(commandes)
print(f"{db.commandes.count_documents({})} documents insérés dans my_base.commandes")

districts = [
    {
        "name": "Mitte",
        "geom": {
            "type": "Polygon",
            "coordinates": [[
                [13.370, 52.505],
                [13.430, 52.505],
                [13.430, 52.540],
                [13.370, 52.540],
                [13.370, 52.505]  # fermé
            ]]
        }
    },
    {
        "name": "Kreuzberg",
        "geom": {
            "type": "Polygon",
            "coordinates": [[
                [13.370, 52.480],
                [13.430, 52.480],
                [13.430, 52.505],
                [13.370, 52.505],
                [13.370, 52.480]
            ]]
        }
    },
    {
        "name": "Potsdam_Area",
        "geom": {
            "type": "Polygon",
            "coordinates": [[
                [13.020, 52.380],
                [13.120, 52.380],
                [13.120, 52.450],
                [13.020, 52.450],
                [13.020, 52.380]
            ]]
        }
    }
]
db.districts.insert_many(districts)
db.districts.create_index([("geom", "2dsphere")])
print(f"{db.districts.count_documents({})} polygones insérés dans my_base.districts")

points_interet = [
    { "name": "POI_Alexanderplatz", "geom": { "type": "Point", "coordinates": [13.413, 52.521] } },
    { "name": "POI_KottbusserTor",  "geom": { "type": "Point", "coordinates": [13.418, 52.499] } },
    { "name": "POI_Tegel",          "geom": { "type": "Point", "coordinates": [13.287, 52.560] } },
    { "name": "POI_Potsdam",        "geom": { "type": "Point", "coordinates": [13.060, 52.400] } },
    { "name": "POI_Out",            "geom": { "type": "Point", "coordinates": [13.500, 52.600] } },
]
db.points_interet.insert_many(points_interet)
db.points_interet.create_index([("geom", "2dsphere")])
print(f"{db.points_interet.count_documents({})} points insérés dans my_base.points_interet")

depots = [
    { "name": "Depot_Nord", "geom": { "type": "Point", "coordinates": [13.390, 52.535] } },
    { "name": "Depot_Sud",  "geom": { "type": "Point", "coordinates": [13.405, 52.485] } }, 
]
db.depots.insert_many(depots)
db.depots.create_index([("geom", "2dsphere")])
print(f"{db.depots.count_documents({})} dépôts insérés dans my_base.depots")

client.close()
