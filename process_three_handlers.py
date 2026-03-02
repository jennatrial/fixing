import logging
from typing import Tuple

from handlers.transform_demarrage import nettoyer_demarrage
from handlers.transform_merge_planning_dispo import merge_planning_dispo
from handlers.transform_positionnement import nettoyer_positionnement
# new imports for merged planning flow
from handlers.transform_merge_planning_dispo import merge_planning_dispo
from utils.insert_merge_planning_dispo import inserer_merge_planning_dispo
from utils.insert_demarrage import inserer_demarrage
from utils.insert_positionnement import inserer_positionnement

# Avoid importing `archiver_fichier` at module import time to prevent circular import with `utils.router`.
# We'll import it locally inside the function when needed.


def process_three_handlers(blob_client, filename: str, container_client) -> dict:
    """
    Sequentially run three transform+insert pairs: Démarrage, Planning Dispo, Positionnement.
    - Each pair runs in its own try/except so one failure doesn't block the others.
    - If at least one pair succeeds, the original blob is archived using `archiver_fichier`.

    Returns a summary dict with results for each step and whether the file was archived.
    """
    results = {}
    any_success = False

    # use the merged planning handler which gracefully handles single or
    # multiple "S" sheets. the original helpers remain available if
    # backwards compatibility is ever required
    steps: Tuple[Tuple[str, object, object], ...] = (
        ("Démarrage", nettoyer_demarrage, inserer_demarrage),
        ("Planning Dispo", merge_planning_dispo, inserer_merge_planning_dispo),
        ("Positionnement", nettoyer_positionnement, inserer_positionnement),
    )

    for label, transform_fn, insert_fn in steps:
        try:
            logging.info(f"🔄 Démarrage du traitement: {label} pour le fichier {filename}")
            df = transform_fn(blob_client)
            if df is None or getattr(df, "empty", False):
                logging.info(f"ℹ️ Aucun enregistrement à insérer pour {label}.")
                results[label] = "no_data"
                continue

            insert_res = insert_fn(df)
            results[label] = {"status": "success", "details": insert_res}
            any_success = True
            logging.info(f"✅ Traitement {label} terminé pour {filename}")

        except Exception as e:
            logging.error(f"❌ Traitement {label} du fichier {filename} a échoué : {e}")
            results[label] = {"status": "error", "error": str(e)}

    # Archive only if none of the steps failed
    archived = False
    # Treat 'no_data' and 'success' as non-failure; archive only when there is no 'error'
    statuses = []
    for v in results.values():
        if isinstance(v, dict) and 'status' in v:
            statuses.append(v['status'])
        else:
            statuses.append(v)

    if all(s != 'error' for s in statuses) and len(statuses) == len(steps):
        try:
            # Local import to avoid circular import with utils.router
            from utils.router import archiver_fichier
            archiver_fichier(blob_client, filename, container_client)
            archived = True
            logging.info(f"📦 Fichier {filename} archivé après traitements (tutti e tre ok/no_data).")
        except Exception as e:
            logging.error(f"❌ Échec de l'archivage pour {filename} : {e}")

    return {"results": results, "archived": archived}


# Optional small CLI helper for manual runs (not executed automatically by app)
if __name__ == "__main__":
    print("This module provides process_three_handlers(blob_client, filename, container_client)")
