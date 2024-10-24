#!bash

# Java
for schema in jsonschema/pipes/*.schema.json; do
    quicktype -s schema -l java -o src/main/java/generated/$(basename $schema .schema.json).java $schema
done
