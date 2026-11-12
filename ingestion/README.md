# \# Ingestion : compteurs intelligents → Kafka

# 

# Responsable : Étudiant 1 (sources de données et ingestion Kafka).

# 

# Cette partie simule des compteurs électriques et envoie leurs relevés dans Kafka.

# 

# ```

# Dataset UCI → simulateur.py → MQTT (Mosquitto) → passerelle.py → Kafka (topic "releves")

# ```

# 

# \## Ce que les autres doivent savoir

# 

# | Élément | Valeur |

# |---|---|

# | Topic Kafka | `releves` |

# | Partitions | 3 |

# | Clé du message | `meter\_id` (ex. `M006`) |

# | Adresse du broker (depuis le PC) | `localhost:9092` |

# | Format des messages | JSON (texte UTF-8) |

# | Nombre de compteurs | 20 (`M001` à `M020`) |

# | Fréquence simulée | 1 relevé toutes les 30 minutes de données |

# 

# \### Exemple de message

# ```json

# {"meter\_id": "M006", "quartier": "Q3", "timestamp": "16/12/2006 17:24:00",

# &#x20;"active\_power": 4.214, "voltage": 234.84, "intensity": 18.4, "is\_anomaly": 0}

# ```

# 

# \### Champs

# 

# | Champ | Type | Signification |

# |---|---|---|

# | `meter\_id` | texte | Identifiant du compteur (`M001` à `M020`) |

# | `quartier` | texte | Quartier du compteur (`Q1` à `Q4`) |

# | `timestamp` | texte | Date et heure au format `jj/mm/aaaa HH:MM:SS` (données de décembre 2006 à 2010) |

# | `active\_power` | nombre | Puissance active en kW (avec un bruit de ±10 % par compteur) |

# | `voltage` | nombre | Tension en volts |

# | `intensity` | nombre | Intensité en ampères |

# | `is\_anomaly` | 0 ou 1 | 1 = anomalie injectée volontairement (consommation ×5, environ 0,2 % des relevés) |

# 

# `is\_anomaly` sert de vérité terrain pour évaluer la détection d'anomalies : il ne faut pas l'utiliser comme variable d'entrée d'un modèle.

# 

# \## Lancer l'ingestion

# 

# Prérequis : Docker Desktop, Python 3.10 ou plus, dataset `household\_power\_consumption.txt` (UCI Individual Household Electric Power Consumption) placé dans `ingestion/data/`.

# 

# 1\. Installer les dépendances (une seule fois) :

# &#x20;  ```

# &#x20;  python -m venv venv

# &#x20;  venv\\Scripts\\activate

# &#x20;  pip install pandas "paho-mqtt>=2.0" kafka-python

# &#x20;  ```

# 2\. Démarrer Mosquitto et Kafka :

# &#x20;  ```

# &#x20;  docker compose up -d

# &#x20;  ```

# 3\. Créer le topic (une seule fois) :

# &#x20;  ```

# &#x20;  docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic releves --bootstrap-server localhost:9092 --partitions 3

# &#x20;  ```

# 4\. Lancer la passerelle (terminal 1, à garder ouvert) :

# &#x20;  ```

# &#x20;  python passerelle.py

# &#x20;  ```

# 5\. Lancer le simulateur (terminal 2) :

# &#x20;  ```

# &#x20;  python simulateur.py 0.05

# &#x20;  ```

# &#x20;  Le nombre est le délai en secondes entre deux pas de temps (0.5 par défaut, 0.05 pour aller vite).

# 

# \*\*Important :\*\* lancer la passerelle \*\*avant\*\* le simulateur. MQTT ne conserve pas les messages si personne n'écoute.

# 

# \## Vérifier que ça marche

# 

# Compter les messages dans le topic :

# ```

# docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic releves

# ```

# Lire quelques messages :

# ```

# docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --topic releves --bootstrap-server localhost:9092 --from-beginning --max-messages 5

# ```

# 

# \## Pour lire le topic depuis Spark

# \- Depuis le PC : `localhost:9092`.

# \- Depuis un conteneur Docker : il faudra ajouter un listener interne à Kafka (par exemple `kafka:29092`) dans le `docker-compose.yml` final. À faire à l'intégration.

# \- Les valeurs des messages sont du JSON : à parser avec `from\_json` et un schéma.

# 

# \## Fichiers

# 

# | Fichier | Rôle |

# |---|---|

# | `simulateur.py` | Lit le CSV UCI et publie les relevés de 20 compteurs sur MQTT |

# | `passerelle.py` | Lit MQTT (`releves/#`) et recopie chaque message dans Kafka |

# | `docker-compose.yml` | Mosquitto et Kafka |

# | `mosquitto.conf` | Configuration de Mosquitto |

# | `test\_kafka.py` | Petit test d'envoi vers Kafka |

