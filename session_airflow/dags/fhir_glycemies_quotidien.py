"""TP séance 2 · Cas d'usage : le flux FHIR quotidien des glycémies.

extraire (API FHIR, pagination) → aplatir (Parquet) → valider (Pandera) → charger (DuckDB, idempotent)
Le guide pas à pas est dans TD.md (partie 04).

Avant de commencer :
  - copier ce fichier dans dags/ ;
  - créer le pool api_fhir : Admin › Pools › + , nom api_fhir, 3 places.
Sans réseau : déclencher avec le paramètre source = fichier (formulaire « Trigger »).
"""
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import Asset, Param, dag, task
from airflow.timetables.trigger import CronTriggerTimetable

DONNEES = Path(__file__).resolve().parents[1] / "donnees"
BASE = "https://hapi.fhir.org/baseR5x"
UNITES = ["g/L", "mg/dL", "mmol/L"]
EXEMPLE = DONNEES / "exemple" / "glycemies_exemple.ndjson"     # repli sans réseau (données de la séance 1)
glycemies = Asset("glycemies")


def jour(data_interval_start) -> str:
    """Le jour traité ; une exécution manuelle n'a pas d'intervalle : on prend la veille."""
    return (data_interval_start or datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def alerter(context):
    """Appelée quand une tâche échoue définitivement (après ses retries)."""
    ti = context["ti"]
    message = f"ALERTE {datetime.now():%H:%M:%S} · {ti.dag_id}.{ti.task_id} en échec : {context.get('exception')}"
    print(message)
    with open(DONNEES / "alertes.log", "a") as f:
        f.write(message + "\n")


@dag(schedule=CronTriggerTimetable("0 6 * * *", timezone="Europe/Paris", interval=timedelta(days=1)),
     start_date=datetime(2026, 11, 1), catchup=False, tags=["fhir", "tp"],
     default_args={"on_failure_callback": alerter},
     params={"source": Param("api", enum=["api", "fichier"],
                             description="api : serveur FHIR public ; fichier : données d'exemple, sans réseau")})
def fhir_glycemies_quotidien():

    # En production : retries=5, retry_delay=2 min ; raccourci ici pour voir la panne pendant le TP
    @task(pool="api_fhir", retries=3, retry_delay=timedelta(seconds=20))
    def extraire(params=None, data_interval_start=None) -> str:
        """Étape 1 : glycémies (LOINC 2345-7) modifiées depuis le début de l'intervalle, toutes pages."""
        import requests

        chemin = DONNEES / "brut" / f"glycemies_{jour(data_interval_start)}.ndjson"
        chemin.parent.mkdir(parents=True, exist_ok=True)
        if params["source"] == "fichier":
            chemin.write_text(EXEMPLE.read_text())
            print("Source fichier :", EXEMPLE)
            return str(chemin)

        url = f"{BASE}/Observation"
        requete = {"code": "http://loinc.org|2345-7", "_count": 100,
                   "_lastUpdated": f"ge{jour(data_interval_start)}"}
        ressources = []
        # À COMPLÉTER (étape 1) : boucle de pagination de la séance 1, qui remplit `ressources`
        #   - r = requests.get(url, params=requete, headers={"Accept": "application/fhir+json"}, timeout=30)
        #   - r.raise_for_status() : une erreur HTTP fait échouer la tâche, donc déclenche un retry
        #   - ajouter les ressources de bundle["entry"], suivre le lien « next », puis requete = None
        raise NotImplementedError("Étape 1 à compléter (ou déclencher avec source = fichier)")
        chemin.write_text("".join(json.dumps(o) + "\n" for o in ressources))
        print(len(ressources), "ressources écrites dans", chemin)
        return str(chemin)                            # XCom : un chemin, pas les données

    @task
    def aplatir(chemin: str) -> str:
        """Transformer les observations FHIR NDJSON en Parquet."""
        import json
        import pandas as pd
        from pathlib import Path

        lignes = []

        with open(chemin, "r", encoding="utf-8") as fichier:
            for ligne in fichier:
                if not ligne.strip():
                    continue

                observation = json.loads(ligne)

                patient = observation.get("subject", {}).get("reference")
                if patient:
                    patient = patient.split("/")[-1]

                quantite = observation.get("valueQuantity") or {}

                lignes.append({
                    "id": observation.get("id"),
                    "patient": patient,
                    "date": observation.get("effectiveDateTime"),
                    "statut": observation.get("status"),
                    "valeur": quantite.get("value"),
                    "unite": quantite.get("unit")
                })

        df = pd.DataFrame(
            lignes,
            columns=["id", "patient", "date", "statut", "valeur", "unite"]
        )

        sortie = Path(chemin).with_name("glycemies_aplaties.parquet")
        df.to_parquet(sortie, index=False)

        print(f"{len(df)} lignes aplaties")
        print(f"Unités : {df['unite'].value_counts(dropna=False).to_dict()}")

        return str(sortie)

        raise NotImplementedError("Étape 2 à compléter")

    
    @task
    def valider(chemin: str) -> str:
        """Étape 3 : validation des glycémies avec Pandera."""
        import pandas as pd
        import pandera.pandas as pa
        from pathlib import Path

        # 1. Lire le fichier Parquet
        df = pd.read_parquet(chemin)

        # 2. Définir les unités autorisées
        unites = ["g/L", "mg/dL", "mmol/L"]

        # 3. Définir un schéma de validation Pandera
        schema = pa.DataFrameSchema({
            "unite": pa.Column(
                str,
                pa.Check.isin(unites),
                nullable=False
            ),
            "valeur": pa.Column(
                float,
                pa.Check.gt(0),
                nullable=False,
                coerce=True
            ),
            "patient": pa.Column(
                str,
                nullable=False
            )
        })

        # 4. Identifier les lignes valides
        valeurs = pd.to_numeric(df["valeur"], errors="coerce")

        masque = (
            df["unite"].isin(unites)
            & valeurs.gt(0)
            & df["patient"].notna()
            & df["patient"].astype("string").str.strip().ne("").fillna(False)
        )

        valides = df.loc[masque].copy()
        rejetees = df.loc[~masque].copy()

        # 5. Convertir les valeurs en nombres
        valides["valeur"] = pd.to_numeric(valides["valeur"])

        # 6. Vérifier les lignes valides avec Pandera
        valides = schema.validate(valides)

        # 7. Enregistrer les données valides
        sortie = Path(chemin).with_name("glycemies_valides.parquet")
        valides.to_parquet(sortie, index=False)

        # 8. Afficher les résultats dans les logs
        print(f"Lignes totales : {len(df)}")
        print(f"Lignes valides : {len(valides)}")
        print(f"Lignes rejetées : {len(rejetees)}")

        return str(sortie)
 
    @task(outlets=[glycemies])
    def charger(chemin: str, data_interval_start=None):
        """Étape 4 : charger les glycémies dans DuckDB sans doublons."""
        import duckdb

        # 1. Déterminer le jour traité
        date_jour = jour(data_interval_start)

        # 2. Définir le chemin de la base de données
        base = DONNEES / "entrepot.duckdb"
        base.parent.mkdir(parents=True, exist_ok=True)

        # 3. Se connecter à DuckDB
        con = duckdb.connect(str(base))

        try:
            # 4. Créer la table si elle n'existe pas
            con.execute("""
                CREATE TABLE IF NOT EXISTS glycemies (
                    id VARCHAR,
                    patient VARCHAR,
                    date VARCHAR,
                    statut VARCHAR,
                    valeur_mg_dl DOUBLE,
                    unite VARCHAR,
                    jour_traitement DATE
                )
            """)

            # 5. Commencer une transaction
            con.execute("BEGIN TRANSACTION")

            # 6. Supprimer les anciennes lignes du jour
            con.execute(
                "DELETE FROM glycemies WHERE jour_traitement = ?",
                [date_jour]
            )

            # 7. Charger le Parquet et convertir les unités
            con.execute("""
                INSERT INTO glycemies
                SELECT
                    id,
                    patient,
                    date,
                    statut,
                    CASE
                        WHEN unite = 'g/L' THEN valeur * 100
                        WHEN unite = 'mmol/L' THEN valeur * 18.0182
                        ELSE valeur
                    END AS valeur_mg_dl,
                    unite,
                    CAST(? AS DATE) AS jour_traitement
                FROM read_parquet(?)
            """, [date_jour, str(chemin)])

            # 8. Confirmer les modifications
            con.execute("COMMIT")

            # 9. Compter les lignes
            nb_jour = con.execute(
                "SELECT COUNT(*) FROM glycemies WHERE jour_traitement = ?",
                [date_jour]
            ).fetchone()[0]

            total = con.execute(
                "SELECT COUNT(*) FROM glycemies"
            ).fetchone()[0]

            print(f"Glycémies chargées : {nb_jour}")
            print(f"Total dans DuckDB : {total}")

        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.close()


    charger(valider(aplatir(extraire())))


fhir_glycemies_quotidien()