# Default verbatim term values

Some datasets suffer poor interpretation because they lack a value for one or more terms.  For example, a dataset containing only plants should have `dwc:kingdom=Plantae`; without that occurrences could match to animal names where homonyms exist.

Corrections and improvements should be made by the data publisher, but sometimes this can take a long time.  The *default values machine tag* allows us to set a value (only in the case that there is no value) for one or more terms for a dataset.

Default term value machine tags belong to the namespace `default-term.gbif.org`, with the name being the short term name (`kingdom`) and the value the required value (`Plantae`).

A search for all these machine tags is here: https://api.gbif.org/v1/dataset?machineTagNamespace=default-term.gbif.org

Or even:
```
curl -Ss http://api.gbif.org/v1/dataset?machineTagNamespace=default-term.gbif.org | jq '.results[] | .key, .title, .machineTags'
```
