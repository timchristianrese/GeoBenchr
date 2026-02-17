CREATE EXTENSION IF NOT EXISTS postgis;

DROP TABLE IF EXISTS depots          CASCADE;
DROP TABLE IF EXISTS points_interet  CASCADE;
DROP TABLE IF EXISTS districts       CASCADE;
DROP TABLE IF EXISTS commandes       CASCADE;
DROP TABLE IF EXISTS voitures        CASCADE;
DROP TABLE IF EXISTS utilisateurs    CASCADE;

CREATE TABLE utilisateurs (
  id                BIGSERIAL PRIMARY KEY,
  nom               TEXT NOT NULL,
  prenom            TEXT NOT NULL,
  age               INT,
  pays              TEXT,
  date_inscription  TIMESTAMP WITHOUT TIME ZONE NOT NULL
);
CREATE UNIQUE INDEX ON utilisateurs (nom, prenom);

CREATE TABLE voitures (
  id        BIGSERIAL PRIMARY KEY,
  owner_id  BIGINT NOT NULL REFERENCES utilisateurs(id) ON DELETE CASCADE,
  marque    TEXT NOT NULL,
  modele    TEXT NOT NULL,
  annee     INT
);

CREATE TABLE commandes (
  id              BIGSERIAL PRIMARY KEY,
  utilisateur_id  BIGINT NOT NULL REFERENCES utilisateurs(id) ON DELETE CASCADE,
  voiture_id      BIGINT NOT NULL REFERENCES voitures(id)     ON DELETE CASCADE,
  montant         NUMERIC(10,2) NOT NULL,
  statut          TEXT NOT NULL,            
  pays_livraison  TEXT NOT NULL,
  date_commande   TIMESTAMP WITHOUT TIME ZONE NOT NULL
);

CREATE TABLE districts (
  id    BIGSERIAL PRIMARY KEY,
  name  TEXT UNIQUE NOT NULL,
  geom  geometry(Polygon, 4326) NOT NULL
);

CREATE TABLE points_interet (
  id    BIGSERIAL PRIMARY KEY,
  name  TEXT UNIQUE NOT NULL,
  geom  geometry(Point, 4326) NOT NULL
);

CREATE TABLE depots (
  id    BIGSERIAL PRIMARY KEY,
  name  TEXT UNIQUE NOT NULL,
  geom  geometry(Point, 4326) NOT NULL
);

CREATE INDEX ON districts      USING GIST (geom);
CREATE INDEX ON points_interet USING GIST (geom);
CREATE INDEX ON depots         USING GIST (geom);

INSERT INTO utilisateurs (nom, prenom, age, pays, date_inscription)
VALUES
  ('Dupont','Alice',    30, 'Allemagne', (TIMESTAMPTZ '2024-06-01 09:00:00+00' AT TIME ZONE 'UTC')),
  ('Dupont','Jean',     28, 'Allemagne', (TIMESTAMPTZ '2024-06-02 10:15:00+00' AT TIME ZONE 'UTC')),
  ('Dupont','Bernard',  45, 'France',    (TIMESTAMPTZ '2024-06-03 11:30:00+00' AT TIME ZONE 'UTC')),
  ('Ratel','Romain',    32, 'France',    (TIMESTAMPTZ '2024-06-04 12:45:00+00' AT TIME ZONE 'UTC')),
  ('Martin','Arthur',   22, 'Allemagne', (TIMESTAMPTZ '2024-06-05 14:00:00+00' AT TIME ZONE 'UTC')),
  ('Martin','Bruno',    42, 'France',    (TIMESTAMPTZ '2024-06-06 15:20:00+00' AT TIME ZONE 'UTC')),
  ('Lefevre','Caroline',27, 'France',    (TIMESTAMPTZ '2024-06-07 16:35:00+00' AT TIME ZONE 'UTC')),
  ('Nguyen','David',    35, 'France',    (TIMESTAMPTZ '2024-06-08 17:50:00+00' AT TIME ZONE 'UTC')),
  ('Bernard','Elodie',  22, 'France',    (TIMESTAMPTZ '2024-06-09 19:05:00+00' AT TIME ZONE 'UTC'));

INSERT INTO voitures (owner_id, marque, modele, annee)
VALUES
  ((SELECT id FROM utilisateurs WHERE nom='Dupont'  AND prenom='Alice'),   'Volkswagen','Golf',    2018),
  ((SELECT id FROM utilisateurs WHERE nom='Dupont'  AND prenom='Jean'),    'BMW',       '320d',    2016),
  ((SELECT id FROM utilisateurs WHERE nom='Dupont'  AND prenom='Bernard'), 'Renault',   'Clio',    2012),
  ((SELECT id FROM utilisateurs WHERE nom='Ratel'   AND prenom='Romain'),  'Peugeot',   '308',     2020),
  ((SELECT id FROM utilisateurs WHERE nom='Martin'  AND prenom='Bruno'),   'Tesla',     'Model 3', 2021),
  ((SELECT id FROM utilisateurs WHERE nom='Lefevre' AND prenom='Caroline'),'Fiat',      '500',     2015),
  ((SELECT id FROM utilisateurs WHERE nom='Nguyen'  AND prenom='David'),   'Audi',      'A3',      2017);

WITH
alice AS  (SELECT id AS uid FROM utilisateurs WHERE nom='Dupont' AND prenom='Alice'),
jean  AS  (SELECT id AS uid FROM utilisateurs WHERE nom='Dupont' AND prenom='Jean'),
bern  AS  (SELECT id AS uid FROM utilisateurs WHERE nom='Dupont' AND prenom='Bernard'),
rom   AS  (SELECT id AS uid FROM utilisateurs WHERE nom='Ratel'  AND prenom='Romain'),
bruno AS  (SELECT id AS uid FROM utilisateurs WHERE nom='Martin' AND prenom='Bruno'),
caro  AS  (SELECT id AS uid FROM utilisateurs WHERE nom='Lefevre' AND prenom='Caroline'),
david AS  (SELECT id AS uid FROM utilisateurs WHERE nom='Nguyen' AND prenom='David')
INSERT INTO commandes (utilisateur_id, voiture_id, montant, statut, pays_livraison, date_commande)
VALUES
  ((SELECT uid FROM alice),
    (SELECT id FROM voitures WHERE owner_id=(SELECT uid FROM alice)),
    120.50, 'payee',   'Allemagne', (TIMESTAMPTZ '2024-06-15 12:00:00+00' AT TIME ZONE 'UTC')),
  ((SELECT uid FROM alice),
    (SELECT id FROM voitures WHERE owner_id=(SELECT uid FROM alice)),
    89.00,  'annulee', 'Allemagne', (TIMESTAMPTZ '2024-06-14 12:00:00+00' AT TIME ZONE 'UTC')),

  ((SELECT uid FROM jean),
    (SELECT id FROM voitures WHERE owner_id=(SELECT uid FROM jean)),
    230.00, 'payee',   'France',    (TIMESTAMPTZ '2024-05-26 12:00:00+00' AT TIME ZONE 'UTC')),

  ((SELECT uid FROM bern),
    (SELECT id FROM voitures WHERE owner_id=(SELECT uid FROM bern)),
    15.00,  'annulee', 'France',    (TIMESTAMPTZ '2024-06-12 12:00:00+00' AT TIME ZONE 'UTC')),

  ((SELECT uid FROM rom),
    (SELECT id FROM voitures WHERE owner_id=(SELECT uid FROM rom)),
    499.00, 'payee',   'France',    (TIMESTAMPTZ '2024-06-10 12:00:00+00' AT TIME ZONE 'UTC')),

  ((SELECT uid FROM bruno),
    (SELECT id FROM voitures WHERE owner_id=(SELECT uid FROM bruno)),
    79.00,  'payee',   'France',    (TIMESTAMPTZ '2024-06-13 12:00:00+00' AT TIME ZONE 'UTC')),

  ((SELECT uid FROM caro),
    (SELECT id FROM voitures WHERE owner_id=(SELECT uid FROM caro)),
    35.50,  'expediee','France',    (TIMESTAMPTZ '2024-06-07 12:00:00+00' AT TIME ZONE 'UTC')),
  ((SELECT uid FROM caro),
    (SELECT id FROM voitures WHERE owner_id=(SELECT uid FROM caro)),
    12.90,  'annulee', 'France',    (TIMESTAMPTZ '2024-06-11 12:00:00+00' AT TIME ZONE 'UTC')),

  ((SELECT uid FROM david),
    (SELECT id FROM voitures WHERE owner_id=(SELECT uid FROM david)),
    999.99, 'payee',   'France',    (TIMESTAMPTZ '2024-06-03 12:00:00+00' AT TIME ZONE 'UTC'));

INSERT INTO districts (name, geom) VALUES
('Mitte',
  ST_SetSRID(
    ST_GeomFromText('POLYGON((13.370 52.505,13.430 52.505,13.430 52.540,13.370 52.540,13.370 52.505))'),
  4326)
),
('Kreuzberg',
  ST_SetSRID(
    ST_GeomFromText('POLYGON((13.370 52.480,13.430 52.480,13.430 52.505,13.370 52.505,13.370 52.480))'),
  4326)
),
('Potsdam_Area',
  ST_SetSRID(
    ST_GeomFromText('POLYGON((13.020 52.380,13.120 52.380,13.120 52.450,13.020 52.450,13.020 52.380))'),
  4326)
);

INSERT INTO points_interet (name, geom) VALUES
('POI_Alexanderplatz', ST_SetSRID(ST_Point(13.413, 52.521), 4326)),
('POI_KottbusserTor',  ST_SetSRID(ST_Point(13.418, 52.499), 4326)),
('POI_Tegel',          ST_SetSRID(ST_Point(13.287, 52.560), 4326)),
('POI_Potsdam',        ST_SetSRID(ST_Point(13.060, 52.400), 4326)),
('POI_Out',            ST_SetSRID(ST_Point(13.500, 52.600), 4326));

INSERT INTO depots (name, geom) VALUES
('Depot_Nord', ST_SetSRID(ST_Point(13.390, 52.535), 4326)),
('Depot_Sud',  ST_SetSRID(ST_Point(13.405, 52.485), 4326));