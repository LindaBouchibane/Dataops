# DataOps Lab

Pipeline de données complet : MinIO → DuckDB → dbt → Streamlit  
Orchestré par Prefect avec validation Soda et tests dbt.

---

## Stack

| Outil | Rôle |
|---|---|
| PostgreSQL | Source OLTP (customers, products) |
| MinIO | Data lake (orders, order_items CSV) |
| DuckDB | Warehouse analytique |
| dbt | Transformations SQL (staging → intermediate → marts) |
| Soda | Validation qualité des données |
| Prefect | Orchestration du pipeline |
| Streamlit | Dashboard de visualisation |

---

## Lancer l'environnement

```bash
# Activer l'environnement virtuel
source dbt-env/bin/activate

# Démarrer les conteneurs Docker (PostgreSQL + MinIO)
docker compose up -d

# Vérifier que les conteneurs tournent
docker compose ps
```

---

## Lancer le pipeline complet

```bash
python flows/ingestion_flow.py
```

Le pipeline exécute automatiquement dans l'ordre :
1. Extraction depuis MinIO et PostgreSQL
2. Validation du schéma CSV (colonnes attendues)
3. Nettoyage des données
4. Chargement dans DuckDB staging
5. Soda checks schéma (colonnes + types)
6. Soda checks qualité (nulls, doublons, valeurs acceptées)

---

## Lancer les transformations dbt

```bash
# Exécuter tous les modèles
dbt run

# Exécuter un modèle spécifique
dbt run --select stg_orders
dbt run --select int_orders_enriched
dbt run --select fct_orders
```

---

## Checker les tests de qualité

### Soda — Validation schéma (colonnes + types)
```bash
soda scan -d duckdb -c soda/configuration.yml soda/checks_schema.yml
```

### Soda — Qualité staging (nulls, doublons, valeurs)
```bash
soda scan -d duckdb -c soda/configuration.yml soda/checks_staging.yml
```

### Soda — Qualité intermediate
```bash
soda scan -d duckdb -c soda/configuration.yml soda/checks_intermediate.yml
```

### dbt — Tous les tests
```bash
dbt test
```

### dbt — Tests par couche
```bash
# Staging uniquement
dbt test --select staging

# Intermediate uniquement
dbt test --select intermediate

# Marts uniquement
dbt test --select marts
```




## Connexions

| PostgreSQL | localhost:5433 | dataops / dataops |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Streamlit | http://localhost:8501 | — |

