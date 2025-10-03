#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Générateur GPX pour données Withings / ScanWatch.

Ce script traite les fichiers CSV exportés par Withings pour générer des
fichiers GPX compatibles avec Strava. Il inclut les extensions `gpxtpx`
pour la fréquence cardiaque, la température et la cadence.

Optimisations apportées :
- Amélioration des performances (notamment sur la recherche de cadence).
- Lisibilité et maintenance du code améliorées.
- Gestion des erreurs plus précise.
- Utilisation de fonctionnalités Python modernes.
"""

import csv
import bisect
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Tuple, Optional, NamedTuple, Sequence
import xml.etree.ElementTree as ET

# ---------- Constantes ----------

# Noms des fichiers sources
ACTIVITIES_FILENAME = 'activities.csv'
HR_FILENAME = 'raw_hr_hr.csv'
LAT_FILENAME = 'raw_location_latitude.csv'
LON_FILENAME = 'raw_location_longitude.csv'
TEMP_FILENAME = 'raw_core_body_temperature_Core body temperature.csv'
CADENCE_FILENAME = 'raw_tracker_steps.csv'

# Clés possibles pour la colonne du type d'activité dans activities.csv
ACTIVITY_TYPE_KEYS = ["Type d'activité", "Type d' activité", "Activity type"]

# Fuseau horaire UTC (standard library, plus moderne que pytz)
UTC_TZ = timezone.utc

# Structures de données pour plus de clarté
DataPoint = Tuple[datetime, float]
Activity = Dict[str, datetime]

class TimeSeriesData(NamedTuple):
    """Structure pour stocker les données temporelles de manière efficace."""
    timestamps: Sequence[datetime]
    values: Sequence[float]

# ---------- Fonctions de lecture des données ----------

def get_unique_activity_types(filepath: Path) -> List[str]:
    """Lit `activities.csv` et renvoie la liste des types d'activité uniques."""
    types = set()
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                for key in ACTIVITY_TYPE_KEYS:
                    if (activity_type := row.get(key, '').strip()):
                        types.add(activity_type)
                        break
    except FileNotFoundError:
        print(f"Erreur : Le fichier d'activités '{filepath}' est introuvable.")
        return []
    return sorted(types)


def read_activities(filepath: Path, activity_type_filter: str,
                    start_date: datetime.date, end_date: datetime.date) -> List[Activity]:
    """Filtre les activités selon le type et la plage de dates."""
    activities: List[Activity] = []
    filter_all = activity_type_filter == "ALL"
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                activity_type = ""
                for key in ACTIVITY_TYPE_KEYS:
                    if (activity_type := row.get(key, '').strip()):
                        break

                if not filter_all and activity_type != activity_type_filter:
                    continue
                try:
                    start = datetime.fromisoformat(row['Début']).astimezone(UTC_TZ)
                    end = datetime.fromisoformat(row['Fin']).astimezone(UTC_TZ)
                    if start_date <= start.date() <= end_date:
                        activities.append({'start': start, 'end': end})
                except (ValueError, KeyError):
                    # Ignore les lignes avec des dates invalides ou des colonnes manquantes
                    continue
    except FileNotFoundError:
        print(f"Erreur : Le fichier d'activités '{filepath}' est introuvable.")
    return activities


def _parse_withings_row(row: List[str]) -> List[DataPoint]:
    """Parse une ligne CSV de type (timestamp, [durations], [values])."""
    points = []
    try:
        ts_str, durs_s, vals_s = row
        current_dt = datetime.fromisoformat(ts_str).astimezone(UTC_TZ)
        durations = [int(x) for x in durs_s.strip('[]').split(',') if x.strip()]
        values = [float(x) for x in vals_s.strip('[]').split(',') if x.strip()]

        for duration, value in zip(durations, values):
            points.append((current_dt, value, duration))
            current_dt += timedelta(seconds=duration)
    except (ValueError, IndexError):
        # Ignore les lignes mal formées
        pass
    return points


def read_expanded_data(filepath: Path) -> Optional[TimeSeriesData]:
    """
    Lit et développe un fichier CSV de données temporelles.
    Chaque valeur est placée au début de son intervalle de temps.
    """
    if not filepath.is_file():
        return None

    all_points: List[DataPoint] = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader, None)  # Skip header
        for row in reader:
            for ts, val, dur in _parse_withings_row(row):
                all_points.append((ts, val))

    if not all_points:
        return None
        
    # Le tri global reste par sécurité, comme dans le script original.
    all_points.sort(key=lambda x: x[0])
    return TimeSeriesData(
        timestamps=[p[0] for p in all_points],
        values=[p[1] for p in all_points]
    )


def read_expanded_cadence_data(filepath: Path) -> Optional[TimeSeriesData]:
    """
    Lit le fichier de cadence et calcule les pas/minute pour chaque intervalle.
    Le timestamp correspond à la fin de l'intervalle.
    """
    if not filepath.is_file():
        return None

    all_points: List[DataPoint] = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader, None) # Skip header
        for row in reader:
            # Le timestamp dans la ligne est le début de la séquence
            start_of_sequence = datetime.fromisoformat(row[0]).astimezone(UTC_TZ)
            current_dt = start_of_sequence
            
            try:
                durs_s, vals_s = row[1], row[2]
                durations = [int(x) for x in durs_s.strip('[]').split(',') if x.strip()]
                values = [float(x) for x in vals_s.strip('[]').split(',') if x.strip()]

                for duration, value in zip(durations, values):
                    end_of_interval = current_dt + timedelta(seconds=duration)
                    if duration > 0:
                        cadence = (value / duration) * 60
                        all_points.append((end_of_interval, cadence))
                    current_dt = end_of_interval
            except (ValueError, IndexError):
                continue
    
    if not all_points:
        return None

    all_points.sort(key=lambda x: x[0])
    return TimeSeriesData(
        timestamps=[p[0] for p in all_points],
        values=[p[1] for p in all_points]
    )

# ---------- Fonctions de traitement des données ----------

def _interpolate_value(target_dt: datetime, data: TimeSeriesData) -> Optional[float]:
    """Interpolation linéaire simple."""
    if not data or not data.timestamps:
        return None
        
    i = bisect.bisect_left(data.timestamps, target_dt)
    if i == 0:
        return data.values[0]
    if i >= len(data.timestamps):
        return data.values[-1]

    t0, v0 = data.timestamps[i-1], data.values[i-1]
    t1, v1 = data.timestamps[i], data.values[i]

    dt_total = (t1 - t0).total_seconds()
    if dt_total == 0:
        return v0
        
    factor = (target_dt - t0).total_seconds() / dt_total
    return v0 + (v1 - v0) * factor


def find_nearest_value(target_dt: datetime, data: TimeSeriesData) -> Optional[float]:
    """Valeur la plus proche dans le temps."""
    if not data or not data.timestamps:
        return None
        
    i = bisect.bisect_left(data.timestamps, target_dt)
    
    # Cas extrêmes
    if i == 0:
        return data.values[0]
    if i >= len(data.timestamps):
        return data.values[-1]

    # Comparer le point précédent et le point actuel
    dt_prev = (target_dt - data.timestamps[i-1]).total_seconds()
    dt_curr = (data.timestamps[i] - target_dt).total_seconds()
    
    return data.values[i-1] if dt_prev < dt_curr else data.values[i]


def apply_temporal_smoothing(data: TimeSeriesData, window_seconds: int = 10) -> TimeSeriesData:
    """Lissage par moyenne mobile temporelle (fenêtre centrée)."""
    if not data or not data.timestamps:
        return data

    smoothed_values: List[float] = []
    half_window = timedelta(seconds=window_seconds / 2.0)
    
    for i, t in enumerate(data.timestamps):
        start_time = t - half_window
        end_time = t + half_window
        
        start_idx = bisect.bisect_left(data.timestamps, start_time)
        end_idx = bisect.bisect_right(data.timestamps, end_time)
        
        window_values = data.values[start_idx:end_idx]
        if not window_values:
            smoothed_values.append(data.values[i])  # Fallback si la fenêtre est vide
        else:
            smoothed_values.append(sum(window_values) / len(window_values))
            
    return TimeSeriesData(timestamps=data.timestamps, values=smoothed_values)


# ---------- Création du GPX ----------

def create_gpx_for_activity(
    lat_data: TimeSeriesData,
    lon_data: TimeSeriesData,
    hr_data: TimeSeriesData,
    temp_data: Optional[TimeSeriesData],
    cadence_data: Optional[TimeSeriesData],
    activity: Activity,
    output_dir: Path,
    activity_type: str
):
    """Crée le fichier GPX pour une activité donnée."""
    start, end = activity['start'], activity['end']

    # Filtrer les points HR pour l'activité et éviter les doublons par seconde
    hr_points_in_activity: List[DataPoint] = []
    last_ts_int = None
    
    start_idx = bisect.bisect_left(hr_data.timestamps, start)
    end_idx = bisect.bisect_right(hr_data.timestamps, end)

    for i in range(start_idx, end_idx):
        dt, hr = hr_data.timestamps[i], hr_data.values[i]
        current_ts_int = int(dt.timestamp())
        if last_ts_int != current_ts_int:
            hr_points_in_activity.append((dt, hr))
            last_ts_int = current_ts_int

    if not hr_points_in_activity:
        print(f"Aucune donnée de fréquence cardiaque pour l'activité du {start.strftime('%Y-%m-%d %H:%M')}")
        return

    gpx = ET.Element(
        'gpx',
        version='1.1', creator='SCANWATCH 2',
        xmlns='http://www.topografix.com/GPX/1/1',
        attrib={'xmlns:gpxtpx': 'http://www.garmin.com/xmlschemas/TrackPointExtension/v2'}
    )
    trk = ET.SubElement(gpx, 'trk')
    ET.SubElement(trk, 'name').text = f"{activity_type} - {start.strftime('%Y-%m-%d')}"
    seg = ET.SubElement(trk, 'trkseg')

    # Boucle principale sur les points de l'activité
    for dt, hr in hr_points_in_activity:
        lat = _interpolate_value(dt, lat_data)
        lon = _interpolate_value(dt, lon_data)
        if lat is None or lon is None:
            continue

        pt = ET.SubElement(seg, 'trkpt', lat=f"{lat:.6f}", lon=f"{lon:.6f}")
        ET.SubElement(pt, 'time').text = dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        ext = ET.SubElement(pt, 'extensions')
        gpxtpx = ET.SubElement(ext, 'gpxtpx:TrackPointExtension')
        ET.SubElement(gpxtpx, 'gpxtpx:hr').text = str(int(hr))

        if temp_data:
            if (temp := find_nearest_value(dt, temp_data)) is not None:
                ET.SubElement(gpxtpx, 'gpxtpx:atemp').text = str(round(temp, 2))
        
        if cadence_data:
            # Recherche de cadence optimisée (O(log N) au lieu de O(N))
            idx = bisect.bisect_right(cadence_data.timestamps, dt)
            if idx > 0:
                last_cadence_dt = cadence_data.timestamps[idx - 1]
                if (dt - last_cadence_dt).total_seconds() <= 60:
                    last_cadence_value = cadence_data.values[idx - 1]
                    ET.SubElement(gpxtpx, 'gpxtpx:cad').text = str(int(last_cadence_value))

    safe_type = activity_type.lower().replace(' ', '_')
    fname = f"SW_{start.strftime('%Y%m%d_%H%M%S')}_{safe_type}.gpx"
    out_path = output_dir / fname
    
    tree = ET.ElementTree(gpx)
    ET.indent(tree, space="  ") # Pour un fichier GPX lisible (pretty-print)
    tree.write(out_path, encoding='utf-8', xml_declaration=True)
    print(f"Fichier GPX créé : {out_path}")


# ---------- Programme principal ----------

def main():
    """Fonction principale du script."""
    print("=== Générateur GPX pour Withings ScanWatch ===")
    
    while True:
        try:
            src_dir_str = input("Dossier source (contenant activities.csv, etc.) : ").strip()
            src_dir = Path(src_dir_str)
            if (src_dir / ACTIVITIES_FILENAME).is_file():
                break
            print("Dossier invalide ou `activities.csv` manquant.")
        except KeyboardInterrupt:
            print("\nProgramme interrompu.")
            return

    out_dir = src_dir / 'export_gpx'
    out_dir.mkdir(exist_ok=True)

    types = get_unique_activity_types(src_dir / ACTIVITIES_FILENAME)
    if not types:
        print("Aucun type d'activité trouvé dans le fichier.")
        return

    print("\nTypes d'activité disponibles :")
    print("  0. TOUTES")
    for i, t in enumerate(types, 1):
        print(f" {i:2}. {t}")
    
    while True:
        try:
            sel = int(input("Votre choix : "))
            if 0 <= sel <= len(types):
                activity_choice = "ALL" if sel == 0 else types[sel-1]
                break
            print("Choix invalide.")
        except ValueError:
            print("Veuillez entrer un nombre.")
        except KeyboardInterrupt:
            print("\nProgramme interrompu.")
            return
            
    while True:
        try:
            sd_str = input("Date de début (YYYY-MM-DD) : ")
            ed_str = input("Date de fin   (YYYY-MM-DD) : ")
            start_date = datetime.strptime(sd_str, "%Y-%m-%d").date()
            end_date = datetime.strptime(ed_str, "%Y-%m-%d").date()
            break
        except ValueError:
            print("Format de date invalide. Veuillez utiliser YYYY-MM-DD.")
        except KeyboardInterrupt:
            print("\nProgramme interrompu.")
            return

    activities = read_activities(src_dir / ACTIVITIES_FILENAME, activity_choice, start_date, end_date)
    if not activities:
        print("Aucune activité trouvée pour les critères sélectionnés.")
        return

    print("\nLecture des fichiers de données...")
    lat_data = read_expanded_data(src_dir / LAT_FILENAME)
    lon_data = read_expanded_data(src_dir / LON_FILENAME)
    hr_data = read_expanded_data(src_dir / HR_FILENAME)
    
    if not (lat_data and lon_data and hr_data):
        print("Erreur : Un ou plusieurs fichiers de données essentiels (GPS, HR) sont manquants ou vides.")
        return
        
    temp_data = read_expanded_data(src_dir / TEMP_FILENAME)
    cadence_data = read_expanded_cadence_data(src_dir / CADENCE_FILENAME)

    print("Lissage des données GPS (fenêtre de 10s)...")
    lat_smoothed = apply_temporal_smoothing(lat_data, 10)
    lon_smoothed = apply_temporal_smoothing(lon_data, 10)

    print(f"\nTraitement de {len(activities)} activité(s)...")
    for act in activities:
        create_gpx_for_activity(
            lat_smoothed, lon_smoothed, hr_data, 
            temp_data, cadence_data, 
            act, out_dir, activity_choice if activity_choice != "ALL" else "activity"
        )

    print(f"\nTerminé. Les fichiers GPX ont été sauvegardés dans : {out_dir}")

if __name__ == "__main__":
    main()
