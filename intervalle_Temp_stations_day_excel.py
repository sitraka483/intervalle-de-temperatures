import xarray as xr
import numpy as np
import pandas as pd

# Fonction pour convertir de Kelvin à Celsius
def kelvin_to_celsius(temperature_k):
    return temperature_k - 273.15

# Lire le fichier CSV contenant les coordonnées des stations météorologiques
stations_df = pd.read_csv("C:/Users/sitra/Desktop/TT_PCG/t2m_hour_nc/station.csv", sep=';')

# Vérifier les colonnes disponibles
print("Colonnes disponibles dans le fichier CSV :", stations_df.columns)


"""
# Charger les fichiers NetCDF dans un seul Dataset xarray
data_path = "C:/Users/sitra/Desktop/TT_PCG/t2m_hour_nc/*.nc"
ds = xr.open_mfdataset(data_path, combine='by_coords')

# Conversion des températures de Kelvin à Celsius
ds['t2m_celsius'] = kelvin_to_celsius(ds['var167'])

# Définir les intervalles de température (par exemple, intervalles de 2°C)
temp_bins = np.arange(-40, 50, 2)
"""
# Chemin du dossier contenant les fichiers NetCDF horaires
netcdf_directory = "C:/Users/sitra/Desktop/TT_PCG/t2m_hour_nc"

def extract_data_for_station(lat, lon, netcdf_directory):
    """
    Extrait les données NetCDF correspondant aux coordonnées d'une station.
    
    lat : latitude de la station
    lon : longitude de la station
    netcdf_directory : chemin vers le répertoire contenant les fichiers NetCDF horaires
    """
    # Liste de tous les fichiers NetCDF dans le répertoire spécifié
    files = [f for f in os.listdir(netcdf_directory) if f.endswith('.nc')]
    
    # Initialisation d'un DataSet vide pour concaténer les données de tous les fichiers
    ds_station = None

    for file in files:
        file_path = os.path.join(netcdf_directory, file)
        ds = xr.open_dataset(file_path)

        # Extraire les données pour les coordonnées spécifiées (lat, lon)
        ds_subset = ds.sel(lat=lat, lon=lon, method="nearest")

        # Concaténer les données extraites dans un seul DataSet
        if ds_station is None:
            ds_station = ds_subset
        else:
            ds_station = xr.concat([ds_station, ds_subset], dim="time")
    
  
   # Convertir les températures de Kelvin en Celsius
    ds_station['t2m_celsius'] = ds_station['t2m'] - 273.15

    return ds_station
    
# Chemin du dossier contenant les fichiers NetCDF horaires
netcdf_directory = "C:/Users/sitra/Desktop/TT_PCG/t2m_hour_nc" 

# Fonction pour calculer les fréquences journalières pour une station
def calculate_daily_frequency(lat, lon, netcdf_directory):
    # Extraire les données NetCDF pour les coordonnées spécifiées
    ds_station = extract_data_for_station(lat, lon, netcdf_directory)
    
    # Resample les données horaires pour obtenir les fréquences journalières
    resampled_data = ds_station['t2m_celsius'].resample(time='1D').count()

    # Calcul des fréquences par intervalles de température (par ex., 2°C)
    bins = range(-40, 50, 2)  # Intervalles de température par pas de 2°C
    daily_freq = resampled_data.groupby_bins('t2m_celsius', bins=bins).size()

    return daily_freq

# Pour chaque station, calculer les fréquences journalières et les sauvegarder en Excel
for _, station in stations_df.iterrows():
    station_id = station['station_id']
    lat, lon = station['latitude'], station['longitude']

    print(f"Calcul des fréquences journalières pour {station_id} (lat: {lat}, lon: {lon})")

    # Calcul des fréquences journalières
    daily_freq = calculate_daily_frequency(lat, lon)

    # Conversion des fréquences en DataFrame
    daily_freq_df = daily_freq.to_dataframe(name='Frequency').reset_index()

    # Sauvegarde des résultats en format Excel
    excel_filename = f"frequences_journalieres_{station_id}.xlsx"
    daily_freq_df.to_excel(excel_filename, index=False)
    print(f"Résultats enregistrés dans {excel_filename}")
