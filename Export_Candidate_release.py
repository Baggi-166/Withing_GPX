#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Générateur GPX pour données Withings / ScanWatch
- GPX compatible Strava avec extensions gpxtpx
- Timestamps en UTC
- HR non interpolée (expansion au début de chaque intervalle)
- Température & cadence : valeur la plus proche (pas d'interpolation)
- GPS lissé (fenêtre 10 s)
"""

import csv
import bisect
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Union
import xml.etree.ElementTree as ET
import pytz

# ---------- Constantes ----------
ACTIVITIES_FILENAME = 'activities.csv'
HR_FILENAME = 'raw_hr_hr.csv'
LAT_FILENAME = 'raw_location_latitude.csv'
LON_FILENAME = 'raw_location_longitude.csv'
TEMP_FILENAME = 'raw_core_body_temperature_Core body temperature.csv'
CADENCE_FILENAME = 'raw_tracker_steps.csv'
UTC_TZ = pytz.utc

DataPoint = Tuple[datetime, float]
Activity = Dict[str, datetime]


# ---------- Fonctions utilitaires ----------

def get_unique_activity_types(filepath: Path) -> List[str]:
    """Lit activities.csv et renvoie la liste des types d'activité."""
    types = set()
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            for key in ["Type d'activité", "Type d' activité", "Activity type"]:
                if key in row and row[key].strip():
                    types.add(row[key].strip())
    return sorted(types)


def read_activities(filepath: Path, activity_type_filter: str,
                    start_date: datetime.date, end_date: datetime.date) -> List[Activity]:
    """Filtre les activités selon le type et la plage de dates."""
    activities: List[Activity] = []
    filter_all = activity_type_filter == "ALL"
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            activity_type = (row.get("Type d'activité")
                             or row.get("Type d' activité")
                             or row.get("Activity type") or "")
            if not filter_all and activity_type != activity_type_filter:
                continue
            try:
                start = datetime.fromisoformat(row['Début']).astimezone(UTC_TZ)
                end = datetime.fromisoformat(row['Fin']).astimezone(UTC_TZ)
            except Exception:
                continue
            if start_date <= start.date() <= end_date:
                activities.append({'start': start, 'end': end})
    return activities


def read_data_expanded(filepath: Path, return_dict: bool = False) -> Optional[Union[List[DataPoint], Dict[datetime, float]]]:
    """
    Développe les lignes du CSV de la forme:
      timestamp,[durations],[values]
    en plaçant **chaque valeur au début de l'intervalle**.
    """
    if not filepath.is_file():
        return None
    if return_dict:
        out: Dict[datetime, float] = {}
    else:
        out: List[DataPoint] = []

    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            try:
                ts, durs_s, vals_s = row
                cur = datetime.fromisoformat(ts).astimezone(UTC_TZ)
                durs = [int(x) for x in durs_s.strip('[]').split(',') if x.strip()]
                vals = [float(x) for x in vals_s.strip('[]').split(',') if x.strip()]
                for d, v in zip(durs, vals):
                    # point au début de l'intervalle
                    if return_dict:
                        out[cur] = v
                    else:
                        out.append((cur, v))
                    cur += timedelta(seconds=d)
            except Exception:
                continue

    # Tri global
    if return_dict:
        return dict(sorted(out.items(), key=lambda kv: kv[0]))
    else:
        return sorted(out, key=lambda x: x[0])


def read_data_expanded_cadence(filepath: Path) -> Optional[List[DataPoint]]:
    """
    Lit le fichier de cadence, calcule la cadence en pas/minute pour chaque intervalle.
    Retourne une liste de tuples (fin_intervalle, cadence).
    """
    if not filepath.is_file():
        return None
    
    out: List[DataPoint] = []

    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            try:
                ts, durs_s, vals_s = row
                current_dt = datetime.fromisoformat(ts).astimezone(UTC_TZ)
                durations = [int(x) for x in durs_s.strip('[]').split(',') if x.strip()]
                values = [float(x) for x in vals_s.strip('[]').split(',') if x.strip()]
                
                for duration, value in zip(durations, values):
                    # Calcul de la cadence moyenne en pas par minute
                    if duration > 0:
                        cadence = (value / duration) * 60
                        current_dt += timedelta(seconds=duration)
                        out.append((current_dt, cadence))
                    
            except Exception:
                continue
    
    return sorted(out, key=lambda x: x[0])


def _interpolate_value(target_dt: datetime, data: List[DataPoint]) -> Optional[float]:
    """Interpolation linéaire simple."""
    if not data:
        return None
    times = [t for t, _ in data]
    i = bisect.bisect_left(times, target_dt)
    if i == 0:
        return data[0][1]
    if i >= len(data):
        return data[-1][1]
    t0, v0 = data[i-1]
    t1, v1 = data[i]
    dt_total = (t1 - t0).total_seconds()
    if dt_total == 0:
        return v0
    factor = (target_dt - t0).total_seconds() / dt_total
    return v0 + (v1 - v0) * factor


def find_nearest_value(target_dt: datetime, data: List[DataPoint]) -> Optional[float]:
    """Valeur la plus proche dans une liste de DataPoint."""
    if not data:
        return None
    times = [t for t, _ in data]
    i = bisect.bisect_left(times, target_dt)
    candidates = []
    if i < len(times):
        candidates.append((abs((times[i] - target_dt).total_seconds()), data[i][1]))
    if i > 0:
        candidates.append((abs((times[i-1] - target_dt).total_seconds()), data[i-1][1]))
    return min(candidates, key=lambda x: x[0])[1] if candidates else None


def apply_smoothing_temporal(data: List[DataPoint], window_seconds: int = 10) -> List[DataPoint]:
    """Lissage moyenne mobile temporelle (fenêtre centrée)."""
    if not data:
        return []
    times = [t for t, _ in data]
    out: List[DataPoint] = []
    half = window_seconds / 2.0
    for i, (t, _) in enumerate(data):
        start = t - timedelta(seconds=half)
        end = t + timedelta(seconds=half)
        s_idx = bisect.bisect_left(times, start)
        e_idx = bisect.bisect_right(times, end)
        window = data[s_idx:e_idx]
        if not window:
            out.append(data[i]) # Fallback si pas de données dans la fenêtre
            continue
        avg = sum(v for _, v in window) / len(window)
        out.append((t, avg))
    return out


# ---------- Création du GPX ----------

def create_gpx_for_activity(lat_data: List[DataPoint], lon_data: List[DataPoint],
                            hr_data: List[DataPoint], temp_data: List[DataPoint],
                            cadence_data: List[DataPoint],
                            activity: Activity, output_dir: Path, activity_type: str):
    """
    Crée le fichier GPX pour une activité.
    """
    start, end = activity['start'], activity['end']

    # HR points filtrés
    hr_points = [(t, v) for t, v in hr_data if start <= t <= end]
    if not hr_points:
        print(f"Aucune HR pour l'activité {start}")
        return

    # GPX root avec namespace gpxtpx
    gpx = ET.Element(
        'gpx',
        {
            'version': '1.1',
            'creator': 'SCANWATCH 2',
            'xmlns': 'http://www.topografix.com/GPX/1/1',
            'xmlns:gpxtpx': 'http://www.garmin.com/xmlschemas/TrackPointExtension/v2'
        }
    )

    trk = ET.SubElement(gpx, 'trk')
    ET.SubElement(trk, 'name').text = f"{activity_type} - {start.strftime('%Y-%m-%d')}"
    seg = ET.SubElement(trk, 'trkseg')

    # Prépare les données de cadence pour une recherche séquentielle
    sorted_cadence_dts = [dt for dt, _ in cadence_data]
    last_cadence_dt = None
    last_cadence_value = None

    # Ajout de chaque point HR
    last_time: Optional[datetime] = None
    for dt, hr in hr_points:
        # dé-duplication: un seul point par seconde
        if last_time and int((dt - last_time).total_seconds()) == 0:
            continue
        last_time = dt

        lat = _interpolate_value(dt, lat_data)
        lon = _interpolate_value(dt, lon_data)
        if lat is None or lon is None:
            continue

        pt = ET.SubElement(seg, 'trkpt', lat=str(lat), lon=str(lon))
        ET.SubElement(pt, 'time').text = dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        ext = ET.SubElement(pt, 'extensions')
        gpxtpx = ET.SubElement(ext, 'gpxtpx:TrackPointExtension')
        ET.SubElement(gpxtpx, 'gpxtpx:hr').text = str(int(hr))

        temp = find_nearest_value(dt, temp_data)
        if temp is not None:
            ET.SubElement(gpxtpx, 'gpxtpx:atemp').text = str(100*round(temp, 2))

        # --- Début de la nouvelle logique de cadence ---
        cadence_keys_until_dt = [k for k in sorted_cadence_dts if k <= dt]
        if cadence_keys_until_dt:
            last_cadence_dt_candidate = cadence_keys_until_dt[-1]
            if last_cadence_dt is None or last_cadence_dt_candidate > last_cadence_dt:
                last_cadence_dt = last_cadence_dt_candidate
                # On utilise la nouvelle liste de tuples (dt, cadence)
                cadence_index = bisect.bisect_left(cadence_data, (last_cadence_dt, -float('inf')))
                last_cadence_value = cadence_data[cadence_index][1]

        if last_cadence_value is not None:
            # Vérifier que la dernière valeur de cadence n'est pas trop ancienne
            if (dt - last_cadence_dt).total_seconds() <= 60:
                cadence_element = ET.SubElement(gpxtpx, 'gpxtpx:cad')
                cadence_element.text = str(int(last_cadence_value))
        # --- Fin de la nouvelle logique de cadence ---

    # Écriture fichier
    safe_type = activity_type.lower().replace(' ', '_')
    fname = f"SW_{start.strftime('%Y%m%d_%H%M%S')}_{safe_type}.gpx"
    out_path = output_dir / fname
    ET.ElementTree(gpx).write(out_path, encoding='utf-8', xml_declaration=True)
    print(f"Fichier GPX créé : {out_path}")


# ---------- Programme principal ----------

def main():
    print("=== Générateur GPX SCANWATCH 2 ===")
    while True:
        src_dir = Path(input("Dossier source (avec activities.csv et raw_*.csv) : ").strip())
        if src_dir.is_dir():
            break
        print("Dossier invalide.")
    out_dir = src_dir / 'export'
    out_dir.mkdir(exist_ok=True)

    types = get_unique_activity_types(src_dir / ACTIVITIES_FILENAME)
    print("\nTypes disponibles :")
    print(" 0. Tous")
    for i, t in enumerate(types, 1):
        print(f" {i}. {t}")
    sel = int(input("Choix : "))
    activity_choice = "ALL" if sel == 0 else types[sel-1]

    sd = datetime.strptime(input("Date début (YYYY-MM-DD) : "), "%Y-%m-%d").date()
    ed = datetime.strptime(input("Date fin   (YYYY-MM-DD) : "), "%Y-%m-%d").date()

    acts = read_activities(src_dir / ACTIVITIES_FILENAME, activity_choice, sd, ed)
    if not acts:
        print("Aucune activité trouvée.")
        return

    # Lecture données brutes
    lat = read_data_expanded(src_dir / LAT_FILENAME)
    lon = read_data_expanded(src_dir / LON_FILENAME)
    hr = read_data_expanded(src_dir / HR_FILENAME)
    temp = read_data_expanded(src_dir / TEMP_FILENAME)
    cadence = read_data_expanded_cadence(src_dir / CADENCE_FILENAME)

    if not (lat and lon and hr):
        print("Latitude/Longitude/HR manquants.")
        return

    print("Lissage GPS (10 s)…")
    lat_s = apply_smoothing_temporal(lat, 10)
    lon_s = apply_smoothing_temporal(lon, 10)

    for act in acts:
        create_gpx_for_activity(lat_s, lon_s, hr, temp or [], cadence or [], act, out_dir, activity_choice)

    print(f"Terminé. Fichiers dans : {out_dir}")

if __name__ == "__main__":
    main()
