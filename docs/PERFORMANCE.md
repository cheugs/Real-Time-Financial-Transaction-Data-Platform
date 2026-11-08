# Résultats de performance

**Français** · [English](en/PERFORMANCE.md)

## Méthode de mesure

```bash
python scripts/run_benchmark.py 100000
```

```bash
python scripts/run_benchmark.py 1000000
```

1. Le producteur normal est arrêté, pour ne mesurer que les événements du test.
2. `producer/benchmark_load.py` envoie N événements valides à Kafka aussi vite que possible. Chaque événement a son propre compte, pour ne pas déclencher la règle de fréquence élevée.
3. Il attend que `batch_log` montre que les N événements ont été insérés, puis lit dans Postgres et dans `stream_progress` :
   - le nombre stocké, le nombre d'identifiants distincts, les événements perdus
   - le **débit de bout en bout** : N ÷ (dernier `processed_at` − premier `kafka_time`)
   - le **débit de Spark** : `processedRowsPerSecond` fourni par Spark
   - la **latence** : `processed_at − kafka_time` (médiane / p95 / max)
   - le **retard Kafka maximal** : les messages en attente dans Kafka
4. `docker stats` est relevé toutes les 5 s pour le CPU et la mémoire de chaque conteneur.

Résultats bruts : [benchmarks/benchmark_100000.json](../benchmarks/benchmark_100000.json) et
[benchmarks/benchmark_1000000.json](../benchmarks/benchmark_1000000.json).

**Configuration :** un ordinateur portable sous Windows 11 avec Docker Desktop (12 CPU, 8 Go pour
Docker). Spark tourne en `local[4]` avec un driver de 2 Go, `maxOffsetsPerTrigger=20000` et un
déclenchement toutes les 5 s. Il y a un seul broker Kafka avec 3 partitions.

## Résultats

| | 100 000 événements | 1 000 000 d'événements |
|---|---|---|
| Événements stockés / perdus / en double | 100 000 / **0** / **0** | 1 000 000 / **0** / **0** |
| Producteur → Kafka | 41 900 événements/s (2,4 s) | 43 900 événements/s (22,8 s) |
| Débit de Spark (moyenne / max par batch) | 4 445 / 5 839 événements/s | 4 366 / 7 782 événements/s |
| **Débit de bout en bout** | **3 725 événements/s** (26,8 s) | **2 986 événements/s** (5 min 35 s) |
| Latence Kafka → Postgres médiane / p95 / max | 10,9 / 20,6 / 24,5 s | 159 / 281 / 312 s |
| Retard maximal du consommateur Kafka | 60 003 messages | 940 005 messages |
| Micro-batchs Spark | 4 | 49 |
| Spark : CPU moyen / max · mémoire max | 103 % / 237 % · 1,3 Go | 111 % / 751 % · 1,8 Go |
| Postgres : CPU moyen / max · mémoire max | 13 % / 51 % · 86 Mo | 53 % / 204 % · 734 Mo |
| Kafka : CPU moyen / max · mémoire max | 40 % / 207 % · 432 Mo | 24 % / 209 % · 1,1 Go |

(100 % de CPU = un cœur.)

**Charge normale** (le producteur à 5 événements/s) : latence médiane = **2,5 s**, p95 = **4,8 s**.
C'est surtout l'intervalle de déclenchement de 5 secondes.

## Comment lire les chiffres de latence

Pendant un test, les N événements sont envoyés dans Kafka en quelques secondes, mais Spark en lit
au maximum 20 000 par micro-batch. La plupart des événements **attendent donc dans Kafka**, et la
longue latence correspond à ce temps d'attente (visible dans le retard Kafka), pas à un traitement
lent. Avec 1 million d'événements, le dernier a attendu environ 5 minutes. Le pipeline n'a jamais
cédé : le retard est monté à 940 000 messages, puis il est redescendu à 0, et chaque événement a
été stocké exactement une fois.

## Goulots d'étranglement

1. **La requête des fenêtres et du dédoublonnage (`volume_per_min`).** Elle prend environ 10 s par
   batch de 20 000 lignes, contre environ 6 s pour l'écriture brute. Chaque batch redistribue les
   données et met à jour l'état de dédoublonnage (une clé par `event_id` pendant 10 minutes) dans
   RocksDB. Avec 1 million d'événements en quelques minutes, cet état contient environ un million
   de clés. Les deux requêtes partagent les mêmes 4 cœurs locaux.
2. **`foreachBatch` + `collect()` dans l'écriture brute.** Chaque batch est rapatrié sur le driver,
   transformé en objets Python, puis inséré avec une seule connexion psycopg2. Ce travail se fait
   sur un seul fil d'exécution, ce qui explique pourquoi le CPU *moyen* de Spark n'est que d'environ
   1,1 cœur alors qu'il peut en utiliser 4.
3. **Les insertions dans Postgres.** Postgres doit tenir 3 index à jour, vérifier les contraintes et
   exécuter `RETURNING`. Il doit aussi servir la reconstruction dbt toutes les 30 s. Il a atteint
   environ 2 cœurs au maximum : occupé, mais pas à sa limite.
4. **Pas un goulot d'étranglement :** Kafka (un seul broker, environ 44 000 événements/s en entrée) et le producteur.

## Ce qui le rendrait plus rapide (non fait, pour garder le projet simple)

- Écrire en parallèle depuis les exécuteurs (`foreachPartition` ou `COPY` Postgres) au lieu de `collect()`.
- Utiliser plus de partitions Kafka et de cœurs Spark, ou un vrai cluster Spark au lieu de `local[4]`.
- Réduire la durée de dédoublonnage (`dropDuplicatesWithinWatermark`) pour diminuer l'état.
- Augmenter `maxOffsetsPerTrigger` pour avoir moins de batchs, mais plus gros. Le débit augmente, mais la latence par batch aussi.
- Utiliser des modèles dbt incrémentaux au lieu de reconstructions complètes.

## Conclusion

À l'échelle d'un projet étudiant, le pipeline gère facilement la charge normale (environ 5
événements/s avec environ 2,5 s de latence). Il a traité des pics de 100 000 et de 1 million
d'événements à environ 3 000 à 3 700 événements/s de bout en bout, **sans aucune transaction
perdue ni en double**.
