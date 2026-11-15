# Pipeline batch YouTube Data API - Big Data

Projet de Taha Hajji, Bouaicha Ibrahim, Ayoub Chenguir et Jdidi soulaimane. Cette livraison couvre les jalons des semaines 1 et 2 de la fiche de projet. Elle prépare la voie froide ; le modèle ML et l'inférence temps réel sont des étapes ultérieures.

Les comptes rendus sont rédigés en LaTeX dans `docs/rapport_semaine_1.tex` et `docs/rapport_semaine_2.tex`. Avec MiKTeX ou TeX Live, les compiler ainsi :

```powershell
New-Item -ItemType Directory -Force output/pdf | Out-Null
pdflatex -interaction=nonstopmode -halt-on-error -output-directory=output/pdf docs/rapport_semaine_1.tex
pdflatex -interaction=nonstopmode -halt-on-error -output-directory=output/pdf docs/rapport_semaine_2.tex
```

Les figures sont dans `docs/figures` : `docker_services.png` est une capture de Docker Desktop ; les fichiers `terminal_*.png` mettent en page les sorties exactes des commandes exécutées avec Docker. Pour actualiser ces dernières après un nouvel essai, démarrer le cluster puis lancer `python tools/capture_evidence.py` avant de recompiler les rapports.

## Architecture

```text
YouTube Data API v3 (search.list -> videos.list)
                 |
                 v
       Python : JSONL + producteur Kafka
                 |
                 v
       Kafka : youtube.videos.raw
                 |
                 v
       Spark batch : validation -> HDFS Parquet (bronze)
                 |
                 v
       Spark SQL : indicateurs HDFS (gold)
```

Une observation correspond à l'état public d'une vidéo au moment de `collected_at`. Son identifiant métier est `video_id`; le couple (`video_id`, `collected_at`) identifie un instantané. Le producteur évite les doublons d'une recherche paginée. Les chiffres de vues, mentions J'aime et commentaires peuvent changer entre deux collectes ou manquer dans la réponse.

## Ressources et limites

- La configuration actuelle vise **1 000 vidéos au maximum**, avec 30 pages au plus par requête. Deux recherches (`big data` et `apache spark`) ont été nécessaires pour atteindre ce volume. `YOUTUBE_MAX_VIDEOS` et `YOUTUBE_MAX_PAGES` permettent de le réduire.
- Deux workers Spark d'un cœur et 512 Mo chacun ; deux DataNodes distincts et réplication HDFS de 2.
- Prévoir environ 6 Go de RAM disponibles pour Docker Desktop à cause des services Java, ainsi que plusieurs Go pour les images. Arrêter le cluster après la séance avec `docker compose down` (sans `-v`, qui supprimerait les données).
- Un essai complet sur la fixture de trois vidéos, puis sur 1 000 vidéos réelles, a été réalisé le 9 novembre 2026 avec deux DataNodes et deux workers actifs.

## Prérequis

- Docker Desktop avec conteneurs Linux et `docker compose`.
- Python 3.11 ou 3.12 recommandé pour le producteur, avec `pip install -r requirements.txt`.
- Une clé YouTube Data API v3 pour les données réelles. Activer l'API dans Google Cloud Console ; conserver la clé dans `.env`, jamais dans Git.

## Démarrage pas à pas (PowerShell)

```powershell
Copy-Item .env.example .env
# Modifier YOUTUBE_API_KEY dans .env pour une collecte réelle.
python -m pip install -r requirements.txt
docker compose up -d
docker compose ps
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic youtube.videos.raw --partitions 1 --replication-factor 1
```

Attendre que les deux DataNodes apparaissent dans `http://localhost:9870` et les deux workers dans `http://localhost:8080`. Initialiser les répertoires HDFS :

```powershell
docker compose exec namenode hdfs dfs -mkdir -p /data/youtube/bronze /data/youtube/gold
docker compose exec namenode hdfs dfsadmin -report
```

Pour une première démonstration sans clé, publier trois enregistrements fictifs :

```powershell
python -m ingestion.youtube_batch --fixture data/demo_videos.jsonl --publish
```

Pour collecter jusqu'à 1 000 vidéos réelles, renseigner `YOUTUBE_API_KEY` dans `.env`, puis lancer :

```powershell
python -m ingestion.youtube_batch --queries "big data,apache spark" --max-pages 30 --max-videos 1000 --output data/youtube_real.jsonl
```

Le script lit automatiquement `YOUTUBE_API_KEY` dans `.env` si la variable n'est pas déjà définie dans PowerShell. Cette première commande écrit uniquement le fichier local ; elle ne nécessite pas Docker.

Pour garder les données réelles séparées de la fixture, publier le fichier déjà collecté dans un topic dédié, puis utiliser une racine HDFS distincte :

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic youtube.videos.real --partitions 1 --replication-factor 1
python -m ingestion.youtube_batch --fixture data/youtube_real.jsonl --output data/youtube_real.jsonl --publish --topic youtube.videos.real
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --driver-memory 512m --executor-memory 512m --total-executor-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/project/batch/kafka_to_hdfs.py --topic youtube.videos.real --output hdfs://namenode:9000/data/youtube_real/bronze
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --driver-memory 512m --executor-memory 512m --total-executor-cores 2 /opt/project/batch/indicators.py --input hdfs://namenode:9000/data/youtube_real/bronze --output hdfs://namenode:9000/data/youtube_real/gold
```

Le 9 novembre 2026, cette procédure a réussi avec 1 000 vidéos réelles : 1 000 messages Kafka, 1 000 lignes archivées, 1 000 vidéos distinctes dans le contrôle qualité, 0 vue manquante et 28 nombres de mentions J'aime manquants. Le fichier local occupe environ 413 Ko. Les recherches ont fourni 529 vidéos pour `big data` et 471 pour `apache spark` après dédoublonnage ; ce lot n'est pas représentatif de toutes les vidéos YouTube.

Ne publier la démonstration et les données réelles dans le même topic que si l'on veut les mélanger dans l'historique. Pour repartir de zéro après une démonstration, supprimer uniquement le topic et les dossiers HDFS concernés, ou créer un nouveau topic.

Archiver les événements Kafka sur HDFS, puis produire les indicateurs :

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --driver-memory 512m --executor-memory 512m --total-executor-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/project/batch/kafka_to_hdfs.py
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --driver-memory 512m --executor-memory 512m --total-executor-cores 2 /opt/project/batch/indicators.py
docker compose exec namenode hdfs dfs -ls -R /data/youtube
```

Le job d'archive lit le topic de son début à sa fin courante et ajoute les instantanés au Parquet. Une relance peut réarchiver des événements ; l'analyse supprime les doublons par (`video_id`, `collected_at`). Ce choix simple convient à une petite démonstration. Une ingestion incrémentale avec offsets ou checkpoint sera nécessaire pour un historique plus grand.

Les [règles YouTube sur les données API publiques](https://developers.google.com/youtube/terms/developer-policies) limitent leur conservation sans autorisation. Pour cette démonstration, supprimer les partitions de collecte âgées de plus de 30 jours et recalculer les indicateurs :

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --driver-memory 512m --executor-memory 512m --total-executor-cores 2 /opt/project/batch/purge_expired.py
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --driver-memory 512m --executor-memory 512m --total-executor-cores 2 /opt/project/batch/indicators.py
```

## Données et indicateurs

Champs collectés : `video_id`, `collected_at`, `published_at`, `title`, `channel_id`, `channel_title`, `category_id`, `duration_iso8601`, `view_count`, `like_count`, `comment_count`, `query`. Aucune donnée personnelle de spectateur n'est collectée. La couche bronze ajoute le topic, la partition, l'offset et l'horodatage Kafka.

La couche gold contient `channels` (nombre de vidéos, vues et mentions J'aime agrégées par chaîne), `published_daily` (vidéos et vues par jour de publication) et `quality` (nombre d'instantanés et valeurs manquantes). Les indicateurs utilisent le dernier instantané connu de chaque vidéo ; les sommes de vues sont des états observés, pas des vues créées pendant la période.

## Coût API et sources

Le projet utilise `search.list` pour trouver les identifiants et `videos.list` pour les détails publics. Les réponses sont paginées avec `nextPageToken`, et les identifiants sont envoyés par lots de 50. La documentation officielle explique les paramètres et quotas : [search.list](https://developers.google.com/youtube/data/reference/rest/v3/search/list), [videos.list](https://developers.google.com/youtube/data/reference/rest/v3/videos/list), [pagination](https://developers.google.com/youtube/data/guides/implementation/pagination). Vérifier les quotas applicables dans le projet Google Cloud avant les collectes répétées.

## Étapes suivantes

Valider le sujet et les données avec l'enseignante, puis définir une cible ML réaliste et une séparation temporelle avant tout entraînement. Les chiffres publics instantanés ne suffisent pas, seuls, à prouver une prévision de popularité future : il faudra plusieurs collectes datées et éviter toute variable connue seulement après l'instant de prédiction.
