# Checklist d'acceptation DE02 : preuves

**Français** · [English](en/CHECKLIST.md)

Pour chaque critère, ce tableau indique où trouver la preuve dans le projet.
<img src="assets/icons/check.svg" width="16" alt="fait"> = fait et vérifié. Les 83 critères sont remplis.

Fichiers de tests :
- `unit` : tests/unit/
- `integ` : tests/integration/test_streaming_scenarios.py
- `e2e` : tests/e2e/test_pipeline.py
- `dbt` : dbt/models/**/schema.yml et dbt/tests/

## 1. Exactitude fonctionnelle

| Critère | | Preuve |
|---|---|---|
| Attributs financiers réalistes | <img src="assets/icons/check.svg" width="16" alt="fait"> | `producer/generator.py` : compte, commerçant, montant (loi log-normale, devise locale), devise selon le pays, statut, horodatages. `unit/test_generator.py::test_events_have_valid_financial_attributes` |
| Identifiant unique | <img src="assets/icons/check.svg" width="16" alt="fait"> | `event_id` UUID, clé primaire dans `raw_transactions`. `test_every_event_has_a_unique_id`, test dbt `unique` |
| Montants, devises et horodatages valides | <img src="assets/icons/check.svg" width="16" alt="fait"> | Validation Spark + CHECK/clé étrangère Postgres + tests dbt `assert_amounts_are_valid`, `assert_no_future_events` |
| Ingestion par Kafka | <img src="assets/icons/check.svg" width="16" alt="fait"> | `e2e::test_valid_events_flow_from_kafka_to_postgres` |
| Traitement continu par Spark | <img src="assets/icons/check.svg" width="16" alt="fait"> | Structured Streaming, déclenchement toutes les 5 s. Table `stream_progress` |
| Agrégations mathématiquement justes | <img src="assets/icons/check.svg" width="16" alt="fait"> | `unit/test_aggregation.py` (nombres, sommes en EUR, bornes des fenêtres, statuts, délais) |
| PostgreSQL stocke les résultats attendus | <img src="assets/icons/check.svg" width="16" alt="fait"> | Les tests e2e comparent les lignes stockées aux événements envoyés |
| Le tableau de bord correspond à la base | <img src="assets/icons/check.svg" width="16" alt="fait"> | `e2e::test_dashboard_figures_match_the_database` (même module SQL, une seule photo de la base) |

## 2. Qualité et intégrité des données

| Critère | | Preuve |
|---|---|---|
| Champs obligatoires manquants détectés | <img src="assets/icons/check.svg" width="16" alt="fait"> | Motifs `missing_*`. `unit/test_validation.py` |
| Montants invalides rejetés | <img src="assets/icons/check.svg" width="16" alt="fait"> | `non_positive_amount`, `amount_too_large` + `CHECK amount > 0` dans la base |
| Horodatages invalides gérés | <img src="assets/icons/check.svg" width="16" alt="fait"> | `invalid_event_time`, `event_time_in_future` |
| Identifiants en double détectés | <img src="assets/icons/check.svg" width="16" alt="fait"> | Clé primaire + `batch_log.duplicates_skipped`. `e2e::test_duplicate_events_are_stored_once` |
| Les doublons ne gonflent pas les agrégats | <img src="assets/icons/check.svg" width="16" alt="fait"> | `dropDuplicates`. `test_duplicates_do_not_inflate_aggregates`, `integ::test_duplicates_across_batches...`, test dbt `assert_windows_not_inflated` |
| Enregistrements invalides isolés | <img src="assets/icons/check.svg" width="16" alt="fait"> | `bad_events` avec `error_reason`. `e2e::test_invalid_events_are_isolated_with_a_reason` |
| Les transformations préservent l'intégrité | <img src="assets/icons/check.svg" width="16" alt="fait"> | Tests dbt sur staging/marts (unicité, non-nullité, relations, EUR = montant × taux) |
| Règles de qualité testées automatiquement | <img src="assets/icons/check.svg" width="16" alt="fait"> | Tests dbt toutes les 30 s + pytest + CI |

## 3. Traitement en temps réel

| Critère | | Preuve |
|---|---|---|
| Pas besoin d'exécution par lots | <img src="assets/icons/check.svg" width="16" alt="fait"> | Requêtes de streaming qui tournent en continu |
| Temps d'événement distinct du temps de traitement | <img src="assets/icons/check.svg" width="16" alt="fait"> | Colonnes `event_time`, `kafka_time`, `processed_at` ; les fenêtres utilisent `event_time` |
| Événements dans le désordre | <img src="assets/icons/check.svg" width="16" alt="fait"> | `integ::test_out_of_order_events_land_in_the_right_window` |
| Événements en retard gérés par watermark | <img src="assets/icons/check.svg" width="16" alt="fait"> | `integ::test_late_event_within_watermark...`, `...beyond_watermark...`, `e2e::test_late_event...` |
| Agrégations par fenêtre exactes | <img src="assets/icons/check.svg" width="16" alt="fait"> | `unit/test_aggregation.py`, tests d'intégration |
| Latence mesurable | <img src="assets/icons/check.svg" width="16" alt="fait"> | `processed_at - kafka_time`. KPI + graphique du tableau de bord. Test de performance (médiane/p95) |
| Débit mesurable | <img src="assets/icons/check.svg" width="16" alt="fait"> | `stream_progress.processed_rows_per_second`. Tableau de bord. Test de performance |
| Retard du consommateur Kafka observable | <img src="assets/icons/check.svg" width="16" alt="fait"> | `stream_progress.kafka_lag` (dernier offset − offset traité). KPI + graphique du tableau de bord |

## 4. Fiabilité et tolérance aux pannes

| Critère | | Preuve |
|---|---|---|
| Reprise après redémarrage | <img src="assets/icons/check.svg" width="16" alt="fait"> | `restart: unless-stopped` + checkpoints. `e2e::test_spark_restart_does_not_lose_or_duplicate_events` |
| Les checkpoints conservent la progression | <img src="assets/icons/check.svg" width="16" alt="fait"> | `integ::test_restart_from_checkpoint_keeps_state` |
| Les traitements en échec sont réessayés | <img src="assets/icons/check.svg" width="16" alt="fait"> | `with_retry()` pour les écritures en base, puis le batch est relancé depuis le checkpoint. `e2e::test_postgres_outage...` |
| Un traitement en double ne corrompt pas la base | <img src="assets/icons/check.svg" width="16" alt="fait"> | Upserts idempotents ; `batch_log` dans la même transaction |
| Événements passés rejouables | <img src="assets/icons/check.svg" width="16" alt="fait"> | `scripts/replay.*`. `e2e::test_replay_rebuilds_the_same_events` |
| Pas de perte silencieuse à la reprise | <img src="assets/icons/check.svg" width="16" alt="fait"> | `failOnDataLoss=true` ; les tests e2e de redémarrage et de panne comptent chaque événement |
| Les changements de schéma ne cassent pas le traitement | <img src="assets/icons/check.svg" width="16" alt="fait"> | Tests v1/v2/v3 en unitaire, intégration et e2e |
| Garanties de livraison documentées et testées | <img src="assets/icons/check.svg" width="16" alt="fait"> | Tableau « Garanties de livraison » du README + les tests ci-dessus |

## 5. Architecture des données

| Critère | | Preuve |
|---|---|---|
| Rôles des composants clairement définis | <img src="assets/icons/check.svg" width="16" alt="fait"> | Tableau des composants du README. Logique (`transformations.py`) séparée des entrées/sorties (`streaming_job.py`) |
| Topics et partitions Kafka bien configurés | <img src="assets/icons/check.svg" width="16" alt="fait"> | `kafka-init` : 3 partitions, clé = account_id, rétention de 7 jours, création automatique désactivée |
| Flux de données traçables | <img src="assets/icons/check.svg" width="16" alt="fait"> | `kafka_partition`/`kafka_offset` sur chaque ligne ; `batch_log`, `stream_progress`, `pipeline_events` |
| Données brutes et transformées séparées | <img src="assets/icons/check.svg" width="16" alt="fait"> | Schémas `public` (brut/flux) et `analytics` (dbt) |
| Tables PostgreSQL bien structurées | <img src="assets/icons/check.svg" width="16" alt="fait"> | Clés primaires, NOT NULL, CHECK, clé étrangère vers `fx_rates` (`sql/init.sql`) |
| Index justifiés | <img src="assets/icons/check.svg" width="16" alt="fait"> | Commentaires dans `init.sql` + partie « Index » du README |
| Transformations dbt modulaires | <img src="assets/icons/check.svg" width="16" alt="fait"> | staging → marts, sources, variables, tests spécifiques |
| L'architecture permet des évolutions | <img src="assets/icons/check.svg" width="16" alt="fait"> | Nouvelle règle = nouveau modèle/variable dbt ; nouveau champ = champ du schéma ; configuration dans `.env` |

## 6. Performance et passage à l'échelle

Voir [PERFORMANCE.md](PERFORMANCE.md).

| Critère | | Preuve |
|---|---|---|
| Au moins 10 000 transactions | <img src="assets/icons/check.svg" width="16" alt="fait"> | Couvert par les tests à 100 000 et 1 million |
| Testé avec 100 000 transactions | <img src="assets/icons/check.svg" width="16" alt="fait"> | `benchmarks/benchmark_100000.json` |
| Test à 1 million d'événements tenté | <img src="assets/icons/check.svg" width="16" alt="fait"> | `benchmarks/benchmark_1000000.json` |
| Débit mesuré en événements/s | <img src="assets/icons/check.svg" width="16" alt="fait"> | Producteur, Spark et bout en bout |
| Latence mesurée | <img src="assets/icons/check.svg" width="16" alt="fait"> | Médiane / p95 / max |
| Consommation des ressources suivie | <img src="assets/icons/check.svg" width="16" alt="fait"> | CPU/mémoire de chaque conteneur avec `docker stats` (dans le JSON) |
| Goulots d'étranglement identifiés | <img src="assets/icons/check.svg" width="16" alt="fait"> | Partie « Goulots d'étranglement » de PERFORMANCE.md |
| Résultats documentés | <img src="assets/icons/check.svg" width="16" alt="fait"> | PERFORMANCE.md |

## 7. Supervision et observabilité

| Critère | | Preuve |
|---|---|---|
| Ingestion supervisée | <img src="assets/icons/check.svg" width="16" alt="fait"> | KPI du tableau de bord, `batch_log`, débit d'entrée |
| Échecs de traitement enregistrés | <img src="assets/icons/check.svg" width="16" alt="fait"> | `pipeline_events.query_failed` + logs du conteneur (si Postgres lui-même est arrêté, seul le log du conteneur le contient) |
| Retard Kafka supervisé | <img src="assets/icons/check.svg" width="16" alt="fait"> | KPI + graphique du tableau de bord |
| Délais de traitement supervisés | <img src="assets/icons/check.svg" width="16" alt="fait"> | Graphiques de latence, délai d'arrivée, événements en retard |
| Doublons comptés | <img src="assets/icons/check.svg" width="16" alt="fait"> | KPI « Duplicates skipped » |
| Événements invalides comptés | <img src="assets/icons/check.svg" width="16" alt="fait"> | KPI + tableau « invalides par motif » |
| Reprises traçables | <img src="assets/icons/check.svg" width="16" alt="fait"> | `pipeline_events` (fresh_start / restart_from_checkpoint / replay_requested) ; `run_id` dans `stream_progress` ; KPI « Spark restarts » |
| Logs suffisants pour analyser une panne | <img src="assets/icons/check.svg" width="16" alt="fait"> | `logging` Python avec numéro de batch, nombres valides/nouveaux/doublons/invalides, nouvelles tentatives |

## 8. Analyse financière

| Critère | | Preuve |
|---|---|---|
| Volumes de transactions calculés | <img src="assets/icons/check.svg" width="16" alt="fait"> | `txn_volume_per_minute.txn_count` |
| Montants totaux calculés | <img src="assets/icons/check.svg" width="16" alt="fait"> | `total_amount_eur` (toujours dans une seule devise) |
| Répartition par statut disponible | <img src="assets/icons/check.svg" width="16" alt="fait"> | `analytics.status_per_minute`, nombres d'acceptés/refusés par fenêtre |
| Volumes par fenêtre de temps disponibles | <img src="assets/icons/check.svg" width="16" alt="fait"> | Fenêtres d'une minute en temps d'événement |
| Différences de devises bien gérées | <img src="assets/icons/check.svg" width="16" alt="fait"> | `fx_rates` + `amount_eur` ; `volume_by_currency` ; règle du montant élevé en EUR. `test_mixed_currencies_are_summed_in_eur` |
| Indicateurs simulés d'activité suspecte générés | <img src="assets/icons/check.svg" width="16" alt="fait"> | `analytics.suspicious_activity` |
| Règles des indicateurs explicables | <img src="assets/icons/check.svg" width="16" alt="fait"> | Chaque ligne contient une phrase `details`. Les seuils sont des variables dbt |
| Aucune prétention de vraie détection de fraude | <img src="assets/icons/check.svg" width="16" alt="fait"> | README, légende du tableau de bord, commentaires SQL |

## 9. Tests et validation

| Critère | | Preuve |
|---|---|---|
| Les tests unitaires passent | <img src="assets/icons/check.svg" width="16" alt="fait"> | 34 tests dans `tests/unit` |
| Les tests d'intégration passent | <img src="assets/icons/check.svg" width="16" alt="fait"> | 7 scénarios de streaming dans `tests/integration` |
| Les tests de bout en bout passent | <img src="assets/icons/check.svg" width="16" alt="fait"> | 11 tests dans `tests/e2e` (9 normaux + 2 lents) |
| Scénarios de doublons testés | <img src="assets/icons/check.svg" width="16" alt="fait"> | Unitaire + intégration + e2e |
| Scénarios de retards testés | <img src="assets/icons/check.svg" width="16" alt="fait"> | Intégration + e2e |
| Scénarios de désordre testés | <img src="assets/icons/check.svg" width="16" alt="fait"> | Intégration |
| Scénarios de panne testés | <img src="assets/icons/check.svg" width="16" alt="fait"> | Redémarrage de Spark, panne de Postgres (e2e), reprise depuis le checkpoint (intégration) |
| Scénarios de rejeu testés | <img src="assets/icons/check.svg" width="16" alt="fait"> | `e2e::test_replay_rebuilds_the_same_events` |
| Scénarios d'évolution du schéma testés | <img src="assets/icons/check.svg" width="16" alt="fait"> | Unitaire + intégration + e2e (v1, v2, v3 inconnue, version absente) |
| Chiffres du tableau de bord validés avec la base | <img src="assets/icons/check.svg" width="16" alt="fait"> | `e2e::test_dashboard_figures_match_the_database` |

## 10. Qualité technique et professionnelle

| Critère | | Preuve |
|---|---|---|
| Code modulaire et maintenable | <img src="assets/icons/check.svg" width="16" alt="fait"> | generator / producer / transformations / streaming_job / queries / app |
| Configuration externalisée | <img src="assets/icons/check.svg" width="16" alt="fait"> | `.env`, variables dbt |
| Aucun secret versionné sur GitHub | <img src="assets/icons/check.svg" width="16" alt="fait"> | `.env` dans `.gitignore` ; seul `.env.example` avec une valeur d'exemple |
| Services Docker reproductibles | <img src="assets/icons/check.svg" width="16" alt="fait"> | Versions des images et des paquets fixées ; un seul `docker compose up` |
| La CI lance les tests automatiquement | <img src="assets/icons/check.svg" width="16" alt="fait"> | `.github/workflows/ci.yml` lance les tests unitaires, d'intégration et de bout en bout à chaque push, et passe à chaque commit |
| Le README explique l'architecture | <img src="assets/icons/check.svg" width="16" alt="fait"> | README |
| Choix techniques et compromis documentés | <img src="assets/icons/check.svg" width="16" alt="fait"> | Partie « Choix techniques et compromis » du README |
| Limites reconnues | <img src="assets/icons/check.svg" width="16" alt="fait"> | Partie « Limites et améliorations possibles » du README |
| Un autre développeur peut reproduire le projet | <img src="assets/icons/check.svg" width="16" alt="fait"> | `cp .env.example .env && docker compose up -d --build` |
