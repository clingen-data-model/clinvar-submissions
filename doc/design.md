# Requirements

Use the clinical assertion entity type (SCV) as the basis for the output. The record we're outputting needs to refer to:

* Subject (variant--
* Trait set
* Submitter Name
* Submission
* Submission date
* Observation/Evidence Lines

Maintain references to, but do not embed:
  * Trait sets
  * VCV, RCV
  * Variation ID
  
Report spec/referential integrity violations/data inconsistencies.

# Design

* Read in stream
* Trigger SCV construction when clinical_assertion is read
* store everything upstream in RocksDB
