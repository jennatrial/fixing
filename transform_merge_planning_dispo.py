import logging
import pandas as pd
import unicodedata
import re
from pandas import errors
from utils.excel_utils import normaliser_colonnes, split_talent
from io import BytesIO



def _canon(s: str) -> str:
    """Normalize sheet name: lowercase, strip accents, spaces."""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def _clean_sheet(xls: pd.ExcelFile, sheet_name: str) -> pd.DataFrame:
    """
    Version cohérente avec 'nettoyer_planning_dispo' :
    - détecte l’en-tête via 'Prenom Nom'
    - normalise les colonnes avec normaliser_colonnes()
    - applique le même mapping des colonnes unnamed_XX -> colonnes réelles
    - fallback intelligent pour date_dispo (détection de colonnes contenant des dates)
    - split prenom_nom -> nom / prenom
    """

    logging.info(f"📄 traitement feuille individuelle : {sheet_name}")

    # 1) Lecture brute pour détecter la ligne d'en-tête
    df_raw = pd.read_excel(
        xls,
        sheet_name=sheet_name,
        header=None,
        engine="openpyxl"
    )
    if df_raw is None or df_raw.shape[0] == 0:
        raise ValueError(f"Feuille '{sheet_name}' vide ou illisible.")

    # Recherche de la cellule contenant "Prenom Nom"
    matches = df_raw[df_raw.eq("Prenom Nom").any(axis=1)]
    if matches.empty:
        raise ValueError(f"⚠️ Pas de colonne 'Prenom Nom' dans la feuille '{sheet_name}'.")
    header_row = matches.index[0]

    logging.info(f"🔎 Ligne d’entête détectée ({sheet_name}) : {header_row}")

    # 2) Relecture avec la bonne ligne d’en-tête
    df = pd.read_excel(
        xls,
        sheet_name=sheet_name,
        header=header_row,
        engine="openpyxl"
    )

    # 3) Nettoyage de base : suppression lignes/colonnes vides + normalisation
    df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")
    df.columns = normaliser_colonnes(df.columns)

    # ---------------------------------------------------------
    # Vérification/renommage de la colonne identifiant (talent_id)
    # ---------------------------------------------------------

    # Si l'utilisateur a déjà renommé la colonne en 'talent id' dans Excel,
    # après normaliser_colonnes -> elle sera 'talent_id' automatiquement.
    if "talent_id" not in df.columns:
        # Rétrocompatibilité: capturer d'éventuels 'unnamed_0', 'unnamed_0_1', etc.
        talent_src = next((c for c in df.columns if re.fullmatch(r"unnamed_0(\_\d+)?", c)), None)
        if talent_src:
            df.rename(columns={talent_src: "talent_id"}, inplace=True)

    # Log de contrôle
    if "talent_id" not in df.columns:
        logging.warning("⚠️ 'talent_id' toujours absent après normalisation/renommage.")
    else:
        logging.info("🆔 Colonne 'talent_id' détectée.")

        logging.info(f"📋 Colonnes normalisées : {list(df.columns)}")


    # ---------------------------------------------------------
    # 4) Unification de toutes les variantes de “prenom nom”
    # ---------------------------------------------------------
    df.columns = [
        "prenom_nom"
        if re.fullmatch(r"prenom[_\s]*nom", c.strip())
        else c
        for c in df.columns
    ]

    # ---------------------------------------------------------
    # 5) Mapping spécifique par feuille pour date_dispo, anglais, mobilite
    # ---------------------------------------------------------
    COLUMN_MAP = {
        "planningdispo": ("unnamed_32", "unnamed_36", "unnamed_27"),
        "s01": ("unnamed_36", "unnamed_40", "unnamed_41"),
        "s02": ("unnamed_35", "unnamed_39", "unnamed_40"),
        "s03": ("unnamed_34", "unnamed_38", "unnamed_39"),
        "s04": ("unnamed_33", "unnamed_37", "unnamed_38"),
        "s05": ("unnamed_32", "unnamed_36", "unnamed_37"),
        "s06": ("unnamed_31", "unnamed_35", "unnamed_36"),
        "s07": ("unnamed_30", "unnamed_34", "unnamed_35"),
        "s08": ("unnamed_35", "unnamed_38", "unnamed_39"),
    }

    canon = sheet_name.lower().replace(" ", "")

    if canon in COLUMN_MAP:
        date_col, anglais_col, mobilite_col = COLUMN_MAP[canon]
        ren = {}

        if date_col in df.columns: ren[date_col] = "date_dispo"
        if anglais_col in df.columns: ren[anglais_col] = "anglais"
        if mobilite_col in df.columns: ren[mobilite_col] = "mobilite"
        df.rename(columns=ren, inplace=True)
    else:
        logging.warning(f"⚠️ Aucun mapping spécifique pour '{sheet_name}'.")

    # enommages communs pour les autres colonnes   
    column_renames = {
        "unnamed_2": "interview",
        "unnamed:_2": "interview",
        "unnamed_3": "grade",
        "unnamed:_3": "grade",
        "unnamed_4": "site",
        "unnamed:_4": "site"
    }

    df.rename(columns=column_renames, inplace=True)

    logging.info(f"📌 Après mapping spécifique: colonnes={list(df.columns)}")

    # 6) Fallback pour détecter 'date_dispo' si non trouvée

    if "date_dispo" not in df.columns:
        logging.warning(f"⚠️ 'date_dispo' introuvable pour {sheet_name}, recherche d’une colonne de dates…")
        for col in df.columns:
            if col.startswith("unnamed"):
                test_date = pd.to_datetime(df[col], format="%d/%m/%Y", errors="coerce")
                if test_date.notna().sum() > 0:
                    df.rename(columns={col: "date_dispo"}, inplace=True)
                    logging.info(f"➡️ Colonne date_dispo détectée automatiquement : {col}")
                    break

    # ---------------------------------------------------------
    # 7) Conversion de la date
    # ---------------------------------------------------------
    if "date_dispo" in df.columns:
        df["date_dispo"] = pd.to_datetime(df["date_dispo"], format="%d/%m/%Y", errors="coerce")



   # 8) Fallback pour détecter 'date_dispo' si non trouvée

    if "interview" not in df.columns:
        logging.warning(f"⚠️ 'interview' introuvable pour {sheet_name}, recherche d’une colonne de interview…")
        for col in df.columns:
            if col.startswith("unnamed:_2"):
                test_date = pd.to_datetime(df[col], format="%d/%m/%Y", errors="coerce")
                if test_date.notna().sum() > 0:
                    df.rename(columns={col: "interview"}, inplace=True)
                    logging.info(f"➡️ Colonne interview détectée automatiquement : {col}")
                    break

    # ---------------------------------------------------------
    # 9) Conversion de la date
    # ---------------------------------------------------------
    if "interview" in df.columns:
        df["interview"] = pd.to_datetime(df["interview"], format="%d/%m/%Y", errors="coerce")


    # Fallback : détection automatique de la colonne des noms si "prenom_nom" manquant
    if "prenom_nom" not in df.columns:
        text_cols = df.select_dtypes(include=["object"]).columns
        for c in text_cols:
        # recherche d’une colonne avec beaucoup de "Prénom Nom"
            if df[c].astype(str).str.contains(r"\b[A-Za-z]+\s+[A-Za-z]+\b").sum() > 5:
                df.rename(columns={c: "prenom_nom"}, inplace=True)
                logging.info(f"ℹ️ Colonne prenom_nom déduite automatiquement : {c}")
                break

    if "prenom_nom" not in df.columns:
        raise ValueError(
            f"❌ Impossible d’identifier la colonne prenom_nom dans '{sheet_name}'."
        )

    # 8) Split de prenom_nom → nom + prenom
    idx = df.columns.get_loc("prenom_nom")
    new_cols = df["prenom_nom"].apply(split_talent)
    new_cols.columns = ["nom", "prenom"]
    df.drop(columns=["prenom_nom"], inplace=True)
    df.insert(idx, "nom", new_cols["nom"])
    df.insert(idx + 1, "prenom", new_cols["prenom"])


    # 9) Conversion des colonnes S7..S35 en valeurs 0/1
    weeks_col = [f"s{i}" for i in range(7, 36)]
    existing = [c for c in weeks_col if c in df.columns]
    
        # Conversion robuste des colonnes S7..S35 en 0/1, même si elles contiennent 'IP+' ou d'autres codes
    for col in existing:
        df[col] = df[col].apply(
        lambda v: 1 if pd.notna(v) and str(v).strip() not in ["", "0", "nan"] else 0
    )

    logging.info(f"Colonnes finales: {list(df.columns)}")
    assert "talent_id" in df.columns, f"❌ 'talent_id' manquant dans '{sheet_name}'"
    assert "date_dispo" in df.columns, f"❌ 'date_dispo' manquant dans '{sheet_name}'"

    logging.info(f"✅ Feuille '{sheet_name}' nettoyée ({len(df)} lignes)")


    logging.info(f"✅ Feuille '{sheet_name}' nettoyée ({len(df)} lignes)")

    return df

def merge_planning_dispo(blob_client):
    logging.info("Fonction de merge appelée pour les plusieurs workbooks Planning dispo")

    blob_data = blob_client.download_blob().readall()
    if len(blob_data) == 0:
        raise ValueError("Le fichier téléchargé est vide.")

    stream = BytesIO(blob_data)
    all_dfs = []

    with pd.ExcelFile(stream, engine="openpyxl") as xls:
        names = sorted(xls.sheet_names, key=lambda s: s.lower())
        order = 0

        for sheet in names:
            canon = _canon(sheet)
            if ("planning" in canon and "dispo" in canon) or re.match(r"^s\d+", canon):
                try:
                    df = _clean_sheet(xls, sheet)
                
                    # ✅ Sécurité : vérifier la présence de 'talent_id'
                    if "talent_id" not in df.columns:
                        raise ValueError(f"'talent_id' manquant après nettoyage pour la feuille {sheet}. Colonnes: {list(df.columns)}")
                
                    df["_sheet_order"] = order
                    all_dfs.append(df)
                    order += 1
                except Exception as exc:
                    logging.warning(f"Impossible de traiter la feuille {sheet}: {exc}")

    if not all_dfs:
        raise ValueError("Aucune feuille valide n'a pu être lue.")

    df_all = pd.concat(all_dfs, ignore_index=True)

    # Week columns normalization
    week_cols = [c for c in df_all.columns if re.fullmatch(r"s\d+", c)]
    for c in week_cols:
        df_all[c] = df_all[c].apply(
            lambda v: 1 if pd.notna(v) and str(v).strip() != "" else 0
        )

    # Aggregation
    def _combine(group: pd.DataFrame) -> pd.Series:
        out = {
            "talent_id": group["talent_id"].iloc[0],
            "date_dispo": group["date_dispo"].iloc[0],
        }

        for col in ["nom","prenom", "interview", "grade", "site", "anglais", "mobilite"]:
            vals = group[col].dropna() if col in group else []
            out[col] = vals.iloc[-1] if len(vals) else None

        for col in week_cols:
            out[col] = group[col].max()

        return pd.Series(out)

    df_agg = (df_all.sort_values(["talent_id", "date_dispo", "_sheet_order"])
                   .groupby(["talent_id", "date_dispo"], as_index=False)
                   .apply(_combine))

    for col in ["anglais", "mobilite"]:
        if col in df_agg.columns:
            df_agg[col] = df_agg[col].fillna("non")

    df_agg.drop(columns=["_sheet_order"], inplace=True, errors="ignore")

    return df_agg