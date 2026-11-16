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
