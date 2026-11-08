# Mettre le tableau de bord en ligne (gratuitement)

**Français** · [English](en/DEPLOYMENT.md)

**En ligne :** https://cheugs-txn-monitor.streamlit.app/

Le pipeline complet (Kafka + Spark) a besoin d'environ 4 Go de mémoire, ce qui dépasse ce que
proposent les hébergements gratuits. La version en ligne fonctionne donc ainsi :

```
votre ordinateur : pipeline complet ──copie──▶ Neon (Postgres gratuit) ◀──lecture── Streamlit Community Cloud (gratuit)
```

Le tableau de bord en ligne affiche une **copie d'une vraie exécution du pipeline**. Il tourne en
*mode démo* : un bandeau indique quand la copie a été faite, et « la dernière heure » est comptée
à partir de ce moment.

Il faut environ 30 minutes. Tout est gratuit, et aucune carte bancaire n'est demandée.

> **Mots de passe :** le mot de passe Neon va seulement dans votre terminal (étape 3) et dans
> l'écran des secrets de Streamlit (étape 5). Ne le mettez jamais dans un fichier du dépôt.

---

## Étape 1 : Mettre le projet sur GitHub

Le projet est déjà sur GitHub :
https://github.com/cheugs/Real-Time-Financial-Transaction-Data-Platform

`.env` (votre mot de passe local) est dans `.gitignore` : il n'est donc pas dans le dépôt.
Après chaque push, l'onglet **Actions** lance automatiquement l'intégration continue (CI).

## Étape 2 : Créer une base de données gratuite sur Neon

1. Allez sur https://neon.tech et créez un compte (vous pouvez utiliser votre compte GitHub).
2. Créez un projet. Nom : `txn-monitor`. Version de Postgres : 16. Région : la plus proche de vous (par exemple *Europe (Frankfurt)*).
3. Sur la page du projet, cliquez sur **Connect**. Gardez la base par défaut (`neondb`) et copiez la **chaîne de connexion**. Elle ressemble à ceci :

```
postgresql://neondb_owner:MOT-DE-PASSE@ep-quelquechose-123456.eu-central-1.aws.neon.tech/neondb?sslmode=require
```

## Étape 3 : Copier une partie des données du pipeline vers Neon

Laissez d'abord tourner la chaîne locale normalement pendant au moins quelques heures
(`docker compose up -d`), pour avoir assez de données. Ensuite, dans PowerShell, dans le dossier du projet :

```powershell
$env:TARGET_DATABASE_URL = "COLLEZ-ICI-LA-CHAINE-DE-CONNEXION-NEON"
```

```powershell
.venv\Scripts\python scripts\export_snapshot.py --hours 6
```

Si la chaîne a été arrêtée un moment (par exemple pendant la nuit), ajoutez `--from-latest` : les
6 heures sont alors comptées à partir de l'événement le plus récent stocké, et non à partir de maintenant.

Le script crée les tables sur Neon, copie les 6 dernières heures de chaque table, enregistre
l'heure de la copie et lance dbt (modèles + 26 tests) sur Neon. Six heures représentent environ
110 000 transactions, bien en dessous de la limite de stockage gratuite de Neon.

## Étape 4 (recommandée) : Un utilisateur en lecture seule pour le tableau de bord

Le site a seulement besoin de **lire** les données. Dans Neon, ouvrez le **SQL Editor** et lancez
ceci, en choisissant votre propre mot de passe :

```sql
create role dashboard_reader with login password 'CHOISISSEZ-UN-MOT-DE-PASSE-SOLIDE';
grant usage on schema public, analytics to dashboard_reader;
grant select on all tables in schema public, analytics to dashboard_reader;
-- dbt recrée les tables analytics : les nouvelles tables doivent aussi être lisibles
alter default privileges in schema analytics grant select on tables to dashboard_reader;
```

## Étape 5 : Déployer le tableau de bord sur Streamlit Community Cloud

1. Allez sur https://share.streamlit.io et connectez-vous avec **GitHub**.
2. Cliquez sur **Create app** → **Deploy a public app from GitHub**.
3. Remplissez :
   - Repository : `cheugs/Real-Time-Financial-Transaction-Data-Platform`
   - Branch : `main`
   - Main file path : `dashboard/app.py`
   - App URL : choisissez un nom, par exemple `txn-monitor-votrenom`
4. Cliquez sur **Advanced settings** :
   - Python version : **3.12**
   - **Secrets** : collez ceci avec les valeurs de Neon (utilisez `dashboard_reader` si vous avez fait l'étape 4) :

```toml
PG_HOST = "ep-quelquechose-123456.eu-central-1.aws.neon.tech"
PG_PORT = "5432"
PG_DB = "neondb"
PG_USER = "dashboard_reader"
PG_PASSWORD = "LE-MOT-DE-PASSE"
PG_SSLMODE = "require"
DEMO_MODE = "1"
```

5. Cliquez sur **Deploy**. Après 2 à 3 minutes, votre tableau de bord est en ligne sur
   `https://txn-monitor-votrenom.streamlit.app`.

## Étape 6 : Ajouter le lien dans le README

En haut de `README.md` (et de `README.en.md`), sous le titre, ajoutez :

```markdown
**Démo en ligne :** https://txn-monitor-votrenom.streamlit.app
```

Ensuite, faites un commit et un push. Streamlit Cloud met l'application à jour automatiquement à chaque push.

---

## Bon à savoir

- **Application en veille :** les applications Streamlit gratuites se mettent en veille après
  quelques jours sans visite. Un visiteur voit alors un bouton pour la réveiller, et l'application
  revient après environ une minute. Ouvrez le lien vous-même avant de l'envoyer à un recruteur.
- **Nouvelle copie :** relancez l'étape 3 quand vous voulez. L'ancienne copie est remplacée.
- **Fonctionnement du mode démo :** `DEMO_MODE=1` fait compter chaque filtre de temps à partir de
  `demo_meta.snapshot_at` au lieu de `now()`, et avance les heures affichées pour que les données
  les plus récentes apparaissent comme « maintenant » (un bandeau le dit). Voir `dashboard/queries.py`.
  Toutes les heures sont en UTC. La version Docker locale ne définit pas `DEMO_MODE` : elle reste en direct.
- **Un problème ?** Dans Streamlit Cloud, ouvrez votre application → **Manage app** (en bas à droite) pour voir les logs.
