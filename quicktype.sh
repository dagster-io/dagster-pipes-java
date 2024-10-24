#!bash

# Java
for schema in jsonschema/pipes/*.schema.json; do
    quicktype -s schema -l java -o src/main/java/types/$(basename $schema .schema.json).java $schema
done
