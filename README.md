# BCE

## Contribution 

Pour lancer le projet vous allez devoir faire les commandes suivantes : 

```bash
# Create network
docker network create airflow

# start services
docker compose up -d

# stop services
docker compose down

```
Assurez-vous que vous avez supprimé les volumes des anciens services avant de les redémarrer.

## Configuration 

Avant de lancer les DAG, assurez-vous que le fichier contenant les données dans le fichier enterprises.csv soit présent dans le répertoire data du projet (/airflow/data) et ajouté dans le même dossier un fichier bce_processed_files.txt vide.

## Lancement des DAGs

Pour lancer les DAGs, vous pouvez utiliser l'interface web Airflow ou exécuter les commandes suivantes :

Vous pouvez accéder à linterface web Airflow à l'adresse suivante : http://localhost:8080

Vous allez chercher dans la bar de recherche "dag" afin d'afficher les DAGs disponibles.

Vous allez activer les DAGs suivants : 
- dag_start
- dag_search_simple
- dag_exemple_scrapping
- dag_bce_to_neo4j


et vous allez lancer le dag_start.
