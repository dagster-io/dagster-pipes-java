#!bash

mkdir -p src/main/generated

for schema in schema/*.schema.json; do
    quicktype -s schema -l java -o src/main/generated/$(basename $schema .schema.json).java $schema
done
