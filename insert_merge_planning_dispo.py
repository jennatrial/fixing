import os
import pyodbc
import logging
import pandas as pd
import re
from utils.excel_utils import clean_text, safe_date

SQL_CONN_STRING = os.environ.get("DatabaseConnection")


def inserer_merge_planning_dispo(df):
    """
    Insert Planning dispo data coming from a merged workbook (S01..S08)
    into the database.  This function is nearly identical to
    ``insert_planning_dispo`` but it automatically detects week columns
    (``s1``, ``s2``, ...) instead of assuming a fixed 7-35 range.  It can be
    used together with :pyfunc:`handlers.merge_planning_dispo.merge_planning_dispo`.
    """
    inserted_count = 0
    ignored_count = 0
    error_count = 0

    # Require `talent_id` and `prenom` columns
    if 'talent_id' not in df.columns:
        raise ValueError("⚠️ Colonne manquante: talent_id")
    if 'date_dispo' not in df.columns:
        raise ValueError("⚠️ Colonne manquante: date_dispo")

    # determine week columns dynamically
    week_cols = [c for c in df.columns if re.fullmatch(r's\d+', c)]

    with pyodbc.connect(SQL_CONN_STRING) as conn:
        conn.autocommit = False  # 🔒 Démarre une transaction
        cursor = conn.cursor()

        for index, row in df.iterrows():
            try:
                # Skip completely empty rows
                if row.isnull().all():
                    logging.info(f"⏭️ Ligne {index} ignorée car vide.")
                    ignored_count += 1
                    continue

            # --- Identifiant unique ---
                talent_id = str(row.get("talent_id", "")).strip()
                if not talent_id:
                    logging.warning(f"⏭️ Ligne {index} ignorée: talent_id vide")
                    ignored_count += 1
                    continue

                date_dispo = safe_date(row.get("date_dispo"))

                # --- Informations descriptives (pas utilisées comme clé) ---
                nom = clean_text(row.get("nom", "")) or ""
                prenom = clean_text(row.get("prenom", "")) or ""

                interview = row.get("interview")
                grade = row.get("grade")
                site = row.get("site")
                anglais = row.get("anglais")
                mobilite = row.get("mobilite")

                # Nettoyage simple texte
                interview = "" if pd.isna(interview) else str(interview).strip()
                grade = "" if pd.isna(grade) else str(grade).strip()
                site = "" if pd.isna(site) else str(site).strip()
                anglais = "" if pd.isna(anglais) else str(anglais).strip()
                mobilite = "" if pd.isna(mobilite) else str(mobilite).strip()

                # --- Préparation des colonnes semaine ---
                days_data = {}
                for col in week_cols:
                    v = row.get(col, 0)
                    if pd.isna(v) or v == "":
                        days_data[col] = 0
                    else:
                        try:
                            days_data[col] = int(v)
                        except Exception:
                            days_data[col] = 0

                # --- Vérification d’existence ---
                if date_dispo is None:
                    cursor.execute("""
                        SELECT id FROM fact_planning_dispo
                        WHERE talent_id = ? AND date_dispo IS NULL
                    """, talent_id)
                else:
                    cursor.execute("""
                        SELECT id FROM fact_planning_dispo
                        WHERE talent_id = ? AND date_dispo = ?
                    """, talent_id, date_dispo)

                existing_record = cursor.fetchone()

                # ----------------------------------------------------------------
                # INSERT
                # ----------------------------------------------------------------
                if not existing_record:

                    columns = [
                        "talent_id", "nom", "prenom", "interview",
                        "grade", "site", "date_dispo", "anglais", "mobilite"
                    ]
                    values = [
                        talent_id, nom, prenom, interview,
                        grade, site, date_dispo, anglais, mobilite
                    ]

                    # Ajouter colonnes semaine
                    for col in week_cols:
                        columns.append(col)
                        values.append(days_data[col])

                    placeholders = ", ".join(["?" for _ in columns])
                    sql = f"""
                        INSERT INTO fact_planning_dispo ({', '.join(columns)})
                        VALUES ({placeholders})
                    """
                    cursor.execute(sql, *values)
                    inserted_count += 1
                    logging.info(f"✅ Insert Planning dispo pour talent_id={talent_id}")

                # ----------------------------------------------------------------
                # UPDATE
                # ----------------------------------------------------------------
                else:
                    update_set = []
                    update_values = []

                    updates = {
                        "nom": nom,
                        "prenom": prenom,
                        "interview": interview,
                        "grade": grade,
                        "site": site,
                        "anglais": anglais,
                        "mobilite": mobilite,
                    }

                    for col, val in updates.items():
                        update_set.append(f"{col} = ?")
                        update_values.append(val)

                    for col in week_cols:
                        update_set.append(f"{col} = ?")
                        update_values.append(days_data[col])

                    # WHERE talent_id + date_dispo
                    if date_dispo is None:
                        update_values.append(talent_id)
                        sql = f"""
                            UPDATE fact_planning_dispo
                            SET {', '.join(update_set)}
                            WHERE talent_id = ?
                            AND date_dispo IS NULL
                        """
                    else:
                        update_values.extend([talent_id, date_dispo])
                        sql = f"""
                            UPDATE fact_planning_dispo
                            SET {', '.join(update_set)}
                            WHERE talent_id = ?
                            AND date_dispo = ?
                        """

                    cursor.execute(sql, *update_values)
                    inserted_count += 1
                    logging.info(f"🔄 Update Planning dispo pour talent_id={talent_id}")

            except Exception as e:
                error_count += 1
                logging.error(f"❌ Erreur ligne {index}: {str(e)}")
                conn.rollback()
                raise

        # Commit global
        try:
            conn.commit()
            logging.info(
                f"💾 Commit effectué : {inserted_count} insérés/mis à jour, "
                f"{ignored_count} ignorés, {error_count} erreurs"
            )
        except Exception as e:
            conn.rollback()
            logging.error(f"❌ Erreur lors du commit: {str(e)}")
            raise

    return {
        "inserted": inserted_count,
        "ignored": ignored_count,
        "errors": error_count,
    }
