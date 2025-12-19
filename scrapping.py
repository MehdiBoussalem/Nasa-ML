import requests
import pandas as pd
import sqlite3
from time import sleep

# ==============================
# CONFIGURATION
# ==============================

API_KEYS = [
    "1LReSZfEwL4sZUT7E40lYeb5EKuWqDyyqAhMiPkQ",  # pages 0 - 899
    "scv9ketGiujKWg4gwoTU7EzAPeWNtXaQKBofmr8x",  # pages 900 - 1799
    "iemAxoZz7pNK3jSCawf8spvWaf4mPKiYNBkMAqxN"   # pages 1800+
]

PAGE_SIZE = 20
OUTPUT_FILE = "asteroides_nasa_complet.csv"
SAVE_EVERY = 50      # sauvegarde toutes les 50 pages
MAX_RETRIES = 10     # retries par page
SLEEP_TIME = 0.3     # anti rate-limit

# ==============================
# FONCTIONS
# ==============================

def get_api_key(page):
    if page < 900:
        return API_KEYS[0]
    elif page < 1800:
        return API_KEYS[1]
    else:
        return API_KEYS[2]


def fetch_page(page, api_key):
    url = "https://api.nasa.gov/neo/rest/v1/neo/browse"
    params = {
        "page": page,
        "size": PAGE_SIZE,
        "api_key": api_key
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def flatten_json(data, parent_key="", sep="."):
    items = {}

    if isinstance(data, dict):
        for key, value in data.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            items.update(flatten_json(value, new_key, sep))

    elif isinstance(data, list):
        for i, value in enumerate(data):
            new_key = f"{parent_key}{sep}{i}"
            items.update(flatten_json(value, new_key, sep))

    else:
        items[parent_key] = data

    return items


# ==============================
# PROGRAMME PRINCIPAL
# ==============================

all_asteroids = []
processed_pages = set()

# 🔍 Récupération dynamique du nombre total de pages
first_page = fetch_page(0, API_KEYS[0])
TOTAL_PAGES = first_page["page"]["total_pages"]

print(f"📄 Total de pages détectées : {TOTAL_PAGES}")

for page in range(TOTAL_PAGES):
    if page in processed_pages:
        continue

    api_key = get_api_key(page)
    success = False

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            data = fetch_page(page, api_key)
            neos = data.get("near_earth_objects", [])

            for neo in neos:
                all_asteroids.append(flatten_json(neo))

            processed_pages.add(page)
            success = True

            print(f"✅ Page {page} OK ({len(neos)} astéroïdes)")
            sleep(SLEEP_TIME)
            break

        except Exception as e:
            print(f"⚠️ Page {page} tentative {attempt}/{MAX_RETRIES} → {e}")
            sleep(3)

    if not success:
        raise RuntimeError(f"❌ ÉCHEC DÉFINITIF SUR LA PAGE {page}")

    # 💾 Sauvegarde intermédiaire
    if page % SAVE_EVERY == 0 and page != 0:
        df_tmp = pd.DataFrame(all_asteroids)
        df_tmp.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
        print(f"💾 Sauvegarde intermédiaire ({len(df_tmp)} lignes)")

# ==============================
# EXPORT FINAL
# ==============================

df = pd.DataFrame(all_asteroids)
df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

print("\n🎉 EXPORT TERMINÉ")
print(f"📊 Astéroïdes exportés : {len(df)}")

# ==============================
# NETTOYAGE ET EXPORT BDD
# ==============================

print("\n🧹 DÉBUT DU NETTOYAGE DES DONNÉES...")

# Sélection des colonnes essentielles
features_physiques = [
    'id',
    'name',
    'is_potentially_hazardous_asteroid',  # La cible (Target)
    'absolute_magnitude_h',               # Luminosité (très important)
    'estimated_diameter.kilometers.estimated_diameter_min',
    'estimated_diameter.kilometers.estimated_diameter_max'
]

# Sélection des données orbitales du PREMIER passage uniquement (.0.)
features_orbitales = [
    'close_approach_data.0.relative_velocity.kilometers_per_hour',
    'close_approach_data.0.miss_distance.kilometers',
    'close_approach_data.0.miss_distance.astronomical', # Utile pour vérifier
    'orbital_data.minimum_orbit_intersection',          # Le fameux MOID (Critère officiel NASA)
    'orbital_data.eccentricity',                        # Forme de l'orbite
    'orbital_data.inclination'                          # Inclinaison par rapport à la Terre
]

# On combine les deux listes
colonnes_a_garder = features_physiques + features_orbitales

# On crée le DataFrame propre
# L'utilisation de .intersection() évite les erreurs si une colonne manque
cols_existantes = df.columns.intersection(colonnes_a_garder)
df_clean = df[cols_existantes].copy()

# On renomme pour avoir des noms "Data Science friendly" (sans points ni rallonges)
nouveaux_noms = {
    'estimated_diameter.kilometers.estimated_diameter_min': 'diameter_min_km',
    'estimated_diameter.kilometers.estimated_diameter_max': 'diameter_max_km',
    'close_approach_data.0.relative_velocity.kilometers_per_hour': 'velocity_kph',
    'close_approach_data.0.miss_distance.kilometers': 'miss_distance_km',
    'close_approach_data.0.miss_distance.astronomical': 'miss_distance_au',
    'orbital_data.minimum_orbit_intersection': 'moid',
    'orbital_data.eccentricity': 'eccentricity',
    'orbital_data.inclination': 'inclination',
    'is_potentially_hazardous_asteroid': 'is_hazardous'
}

df_clean = df_clean.rename(columns=nouveaux_noms)

# On supprime les quelques lignes qui n'auraient pas de données orbitales (NaN)
df_clean = df_clean.dropna()

print(f"✅ Nouvelle taille du Dataset nettoyé : {df_clean.shape}")
print(df_clean.head())

# Connexion à la base de données (elle sera créée si elle n'existe pas)
conn = sqlite3.connect('asteroids.db')

# Export du DataFrame vers la table 'asteroid'
# if_exists='replace' écrase la table si elle existe déjà, 'append' ajoute les données
df_clean.to_sql('asteroid', conn, if_exists='replace', index=False)

print("✅ Base de données 'asteroids.db' mise à jour et table 'asteroid' remplie avec succès.")

# Fermeture de la connexion
conn.close()
