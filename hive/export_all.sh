#!/bin/bash
# A lancer DANS le conteneur hiveserver2 :
#   docker exec hiveserver2 bash /opt/scripts/export_all.sh
# Execute chaque requete de queries/*.hql et ecrit un CSV dans /export (= dashboard/data/)
URL="jdbc:hive2://localhost:10000/accidents"
OUT=/export
mkdir -p "$OUT"
for f in /opt/scripts/queries/*.hql; do
  name=$(basename "$f" .hql)
  echo ">>> $name"
  beeline -u "$URL" --silent=true --outputformat=csv2 --showHeader=true -f "$f" \
      > "$OUT/$name.tmp" 2> "$OUT/$name.err"
  if [ $? -eq 0 ] && [ -s "$OUT/$name.tmp" ]; then
    mv "$OUT/$name.tmp" "$OUT/$name.csv"; echo "    OK"
  else
    echo "    ECHEC - voir dashboard/data/$name.err"
  fi
done
echo "Termine. Rafraichir le dashboard : http://localhost:8050"
