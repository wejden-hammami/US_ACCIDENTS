"""Genere des CSV SYNTHETIQUES dans sample_data/ (meme colonnes que les requetes Hive)
pour previsualiser le dashboard avant que Hive fonctionne. Ne pas presenter ces chiffres."""
import os, random
import pandas as pd
random.seed(1)
out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sample_data")
os.makedirs(out, exist_ok=True)
def w(n, df): df.to_csv(os.path.join(out, n + ".csv"), index=False)
states = ["CA","TX","FL","SC","NY","NC","VA","PA","MN","OR","IL","GA","WA","MD","AZ","OH"]
w("q01_kpis", pd.DataFrame([dict(accidents=1000000, ids_uniques=1000000, etats=49, gravite_moyenne=2.1,
   premiere_date="2016-02-08 00:00:00", derniere_date="2019-03-31 00:00:00")]))
w("q02_par_etat", pd.DataFrame(dict(state=states, accidents=[300000-i*17000 for i in range(16)], gravite_moyenne=[2.1]*16)))
w("q03_gravite", pd.DataFrame(dict(severity=[1,2,3,4], accidents=[9000,810000,150000,31000])))
w("q04_par_heure", pd.DataFrame(dict(heure=range(24), accidents=[int(20000+30000*(h in (7,8,16,17))+random.randint(0,5000)) for h in range(24)])))
w("q05_par_mois", pd.DataFrame(dict(mois=[f"2017-{m:02d}" for m in range(1,13)], accidents=[random.randint(20000,40000) for _ in range(12)])))
w("q06_meteo", pd.DataFrame(dict(weather_condition=["Fair","Clear","Mostly Cloudy","Overcast","Light Rain","Cloudy","Partly Cloudy","Fog"], accidents=[250000,150000,90000,80000,40000,35000,30000,9000], gravite_moyenne=[2.1,2.1,2.0,2.2,2.2,2.1,2.0,2.4])).head(8))
w("q07_villes", pd.DataFrame(dict(city=["Los Angeles","Miami","Houston","Charlotte","Dallas"], state=["CA","FL","TX","NC","TX"], accidents=[28000,24000,20000,18000,17000])))
w("q08_visibilite", pd.DataFrame(dict(ordre=[1,2,3,4], classe=["< 1 mi","1 - 5 mi","5 - 10 mi","10 mi et +"], accidents=[20000,90000,200000,690000], gravite_moyenne=[2.4,2.2,2.1,2.1])))
w("q09_temperature", pd.DataFrame(dict(ordre=[1,2,3,4,5], classe=["< 32 F","32 - 50 F","50 - 70 F","70 - 90 F","90 F et +"], accidents=[60000,200000,340000,360000,40000], gravite_moyenne=[2.3,2.2,2.1,2.1,2.0])))
w("q10_carte_echantillon", pd.DataFrame(dict(start_lat=[random.uniform(26,48) for _ in range(800)], start_lng=[random.uniform(-122,-72) for _ in range(800)], severity=[random.choice([2,2,2,2,3,3,4,1]) for _ in range(800)])))
w("q11_sources", pd.DataFrame(dict(source=["sqoop","kafka","flume"], lignes=[1000000]*3, ids_uniques=[1000000]*3)))
