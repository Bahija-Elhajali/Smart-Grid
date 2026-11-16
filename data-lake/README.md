# Smart Grid — Data Lake & Apache Hudi



## 1. Objectif



Ce module assure le stockage, la transformation et la préparation

des données de consommation électrique du projet Smart Grid.



Il utilise Apache Spark, Apache Hudi et un stockage S3-compatible

(SILO) pour organiser les données en trois couches : Bronze,

Silver et Gold.



## 2. Architecture



MQTT → Kafka → Bronze (SILO/S3)

                  ↓

              Spark SQL

                  ↓

              Silver (Hudi)

                  ↓

              Spark SQL

                  ↓

              Gold (Hudi)

                  ↓

          Analyse / ML / Dashboard



## 3. Technologies



- Python

- Apache Spark 3.5.1

- Apache Hudi 1.0.2

- Apache Kafka 3.8.0

- SILO (API compatible S3)

- Docker



## 4. Organisation du Data Lake



Bucket : smartgrid-lake



- bronze/releves/ : messages Kafka bruts au format JSON

- silver/mesures_hudi : mesures validées, table Hudi

- gold/consommation_quartier : indicateurs agrégés, table Hudi

- checkpoints/bronze_kafka/ : checkpoints Spark Streaming

- tests/hudi_versioning : table expérimentale de versioning



## 5. Couche Bronze



Le script bronze/kafka_to_bronze.py lit le topic Kafka

"releves" et enregistre les messages JSON dans SILO.



Il conserve notamment :

- raw_json

- topic

- partition

- offset

- kafka_timestamp



La lecture utilise Spark Structured Streaming.



## 6. Couche Silver



Le script silver/bronze_to_silver.py :

- Parse les messages JSON.

- Convertit les dates au format ISO ou JJ/MM/AAAA.

- Vérifie les champs obligatoires.

- Écarte les valeurs électriques négatives.

- Génère une clé unique par compteur et horodatage.

- Prépare les données pour Apache Hudi.



Table Hudi : mesures_silver

Type : COPY_ON_WRITE

Opération prévue : UPSERT



IMPORTANT :

L'écriture automatique Silver reste désactivée

(ALLOW_SILVER_WRITE = False) jusqu'à la validation

d'une stratégie de versioning compatible avec

les données existantes.



## 7. Couche Gold



Le script gold/silver_to_gold.py agrège les mesures

Silver par quartier et par date.



Indicateurs :

- nombre_mesures

- puissance_moyenne

- puissance_max



Table Hudi : consommation_quartier

Type : COPY_ON_WRITE

Opération : UPSERT



## 8. Tests réalisés



- Ingestion Kafka vers Bronze : réussie.

- Lecture et validation Bronze vers Silver : réussies.

- Lecture Silver Hudi : réussie.

- Correction Hudi par UPSERT : réussie.

- Agrégation et écriture Gold : réussies.

- Lecture et vérification Gold : réussies.

- Test de versioning sur table séparée : réussi.



Le test expérimental a confirmé :

revision 1 (2.45) → revision 2 (2.90)

→ revision 1 (2.45)



Résultat final : revision 2, puissance 2.90.



Ce test ne constitue pas encore une migration

du pipeline Silver existant.



## 9. Exécution locale



Prérequis :

- Docker Desktop

- Python

- Conteneur Spark configuré

- Kafka et SILO accessibles

- JARs Hudi, Hadoop AWS et Kafka installés dans Spark



Depuis la racine du projet :



python data-lake/bronze/run_bronze.py



python data-lake/silver/run_silver.py



python data-lake/gold/run_gold.py



python data-lake/hudi/run_read_silver.py



Les scripts Silver et Gold sont en mode DRY_RUN

par défaut. Les écritures nécessitent une

activation explicite et des vérifications préalables.



## 10. Sécurité



Les identifiants SILO sont chargés depuis

les variables d'environnement MINIO_ROOT_USER

et MINIO_ROOT_PASSWORD.



Le fichier .env ne doit jamais être publié

sur GitHub.



Les fichiers de test locaux, les JARs et les

données générées ne doivent pas être commités.



## 11. Travaux restants



- Définir un versioning métier fiable pour les corrections.

- Adapter le pipeline Silver à ce versioning.

- Valider l'intégration avec le simulateur MQTT.

- Préparer l'orchestration avec Airflow.

- Finaliser les tests de bout en bout.


---

## 12. Integration V2 - Silver, Gold et Quarantine

Cette version d'integration conserve les tables Hudi historiques
et utilise des chemins separes pour les nouvelles tables.

### Chemins SILO / S3

Bucket : `smartgrid-lake`

| Couche | Chemin |
|---|---|
| Bronze | `bronze/releves/` |
| Silver V2 | `silver/mesures_hudi_v2` |
| Gold V2 | `gold/consommation_quartier_v2` |
| Quarantine V2 | `quarantine/silver_conflicts_v2` |

### Scripts principaux

- `silver/bronze_to_silver_v2.py` : validation, deduplication,
  detection des conflits et ecriture Hudi controlee.
- `gold/silver_to_gold_v2.py` : agregation des consommations
  par quartier et date.
- `hudi/verify_quarantine_v2.py` : lecture et verification
  des enregistrements en quarantaine.

### Gestion des conflits

Les mesures partageant la meme cle metier et la meme revision,
mais ayant des valeurs differentes, sont exclues de Silver V2.

Les variantes conflictuelles peuvent etre stockees dans
une table Hudi Quarantine avec :

- `quarantine_key` : identifiant calcule a partir du contenu.
- `reason` : motif du rejet.
- `quarantined_at` : horodatage de mise en quarantaine.
- `quarantine_version` : valeur de precombine Hudi.

L'ecriture Quarantine exige :

- `DRY_RUN=false`
- `ALLOW_QUARANTINE_WRITE=true`

L'ecriture Silver reste protegee par
`ALLOW_SILVER_WRITE=False`.

### Resultats verifies

- Integration MQTT vers Kafka puis Bronze : reussie.
- Silver V2 : 20 mesures valides.
- Gold V2 : 4 quartiers, avec 5 mesures chacun.
- Detection : 2 variantes conflictuelles pour M099.
- Persistance et lecture Quarantine Hudi : 2 enregistrements.
- Test de correction Hudi sur table isolee : reussi.

### Limites connues

- La detection des conflits est actuellement intra-batch.
- La gestion persistante des enregistrements invalides
  n'est pas encore implementee.
- L'idempotence Quarantine apres plusieurs executions
  n'a pas encore ete verifiee.
- Le test de versioning Hudi a ete effectue sur une table
  isolee, sans migration des tables historiques.
- Les donnees SILO sont locales et ne sont pas incluses
  dans le depot GitHub.

### Collaboration

Cette integration utilise les fichiers d'ingestion
fournis par Bahija El Hajali. Le travail Data Lake V2
et l'integration sont developpes sur la branche
`noura-abakri-data-lake-hudi-integration`.

Les branches originales restent independantes.
