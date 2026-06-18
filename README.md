# Projet – Sales Data Analytics avec Apache Spark

Ce projet analyse le dataset **Online Retail** avec Apache Spark/PySpark et stocke les données dans HDFS.

## Objectifs

- Charger un dataset e-commerce.
- Nettoyer les données : annulations, quantités négatives, prix invalides, valeurs manquantes.
- Calculer des KPI business.
- Analyser les ventes par mois, produit, pays et client.
- Proposer une segmentation client simple avec RFM.
- Sauvegarder les résultats dans `output/`.

## Structure

```text
sales_data_spark_project/
├── app/
│   └── sales_analytics.py
├── data/
│   └── OnlineRetail.csv #a ajouter 
├── output/               
├── docker-compose.yml
├── hadoop.env #a ajouter 
└── README.md
```

## Installation

1. Télécharger le dataset Online Retail depuis UCI.
2. Convertir le fichier Excel en CSV si nécessaire.
3. Placer le fichier dans :

```bash
data/OnlineRetail.csv
```

4. Lancer les conteneurs :

```bash
docker compose up -d
```

5. Copier le fichier CSV dans HDFS :

```bash
docker exec -it namenode hdfs dfs -mkdir -p /data
docker exec -it namenode hdfs dfs -put -f /data/OnlineRetail.csv /data/OnlineRetail.csv
docker exec -it namenode hdfs dfs -ls /data
```

6. Lancer l'application Spark :

```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/sales_analytics.py
```

## Interfaces utiles

- Spark Master UI : http://localhost:8080
- Spark Worker UI : http://localhost:8081
- HDFS NameNode UI : http://localhost:9870

## Analyses réalisées

- KPI globaux : chiffre d'affaires total, nombre de commandes, nombre de clients, panier moyen.
- Chiffre d'affaires par mois.
- Top produits par chiffre d'affaires.
- Top produits par quantité vendue.
- Ventes par pays.
- Top clients.
- Segmentation RFM simple.


