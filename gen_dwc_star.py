#!/usr/bin/env python3
"""
Generates a frictionless DwC star-schema datapackage with controlled FK violations.

Usage:
  python gen_dwc_star.py \\
    --occurrences 100_000_000 \\
    --datasets    10_000 \\
    --taxa        50_000 \\
    --agents      5_000 \\
    --error-rate  0.01 \\
    --out         ./dwc_star \\
    --workers     16 \\
    --chunk       1_000_000
"""
import argparse, csv, json, os, random, uuid
from multiprocessing import Pool
from mimesis import Field
from mimesis.locales import Locale

# ---------------------------------------------------------------------------
# Dimension generators — run once, IDs shared across workers via files
# ---------------------------------------------------------------------------

BASIS    = ["HumanObservation","MachineObservation","PreservedSpecimen","LivingSpecimen"]
RANKS    = ["SPECIES","GENUS","FAMILY"]
KINGDOMS = ["Animalia","Plantae","Fungi","Bacteria","Chromista"]
RIGHTS   = ["CC0","CC_BY","CC_BY_NC"]
ROLES    = ["Collector","Identifier","Observer","Curator"]
COUNTRIES= ["DK","SE","NO","DE","FR","GB","US","BR","AU","ZA"]

def gen_datasets(n):
  _ = Field(locale=Locale.EN)
  rows = []
  for _ in range(n):  # noqa: F841 (reuse _ is intentional)
    f = Field(locale=Locale.EN)
    rows.append({
      "datasetID":    str(uuid.uuid4()),
      "datasetName":  f("text.title"),
      "ownerOrg":     f("finance.company"),
      "license":      random.choice(RIGHTS),
      "pubDate":      f("datetime.date", start=2000, end=2024),
    })
  return rows

def gen_taxa(n):
  rows = []
  for _ in range(n):
    f = Field(locale=Locale.EN)
    genus   = f("person.last_name")
    species = f("person.last_name").lower()
    rows.append({
      "taxonID":          str(uuid.uuid4()),
      "scientificName":   f"{genus} {species}",
      "taxonRank":        random.choice(RANKS),
      "kingdom":          random.choice(KINGDOMS),
      "class":            f("text.word"),
      "order":            f("text.word"),
      "family":           f("text.word"),
    })
  return rows

def gen_agents(n):
  rows = []
  for _ in range(n):
    f = Field(locale=Locale.EN)
    rows.append({
      "agentID":      str(uuid.uuid4()),
      "fullName":     f("full_name"),
      "role":         random.choice(ROLES),
      "email":        f("email"),
      "orcid":        f("person.identifier", mask="####-####-####-####"),
    })
  return rows

# ---------------------------------------------------------------------------
# Occurrence chunk writer
# ---------------------------------------------------------------------------

def write_occurrence_chunk(args):
  chunk_id, n_rows, out_dir, dataset_ids, taxon_ids, agent_ids, error_rate = args

  # Pre-generate invalid IDs for FK violations — deliberately not in dimension tables
  n_bad = max(1, int(n_rows * error_rate))
  bad_dataset = [str(uuid.uuid4()) for _ in range(max(1, n_bad // 3))]
  bad_taxon   = [str(uuid.uuid4()) for _ in range(max(1, n_bad // 3))]
  bad_agent   = [str(uuid.uuid4()) for _ in range(max(1, n_bad // 3))]

  path = os.path.join(out_dir, f"occurrence_chunk_{chunk_id:04d}.csv")
  fields = [
    "occurrenceID","datasetID","taxonID","agentID",
    "decimalLatitude","decimalLongitude","eventDate",
    "basisOfRecord","countryCode","individualCount",
    "coordinateUncertaintyInMeters","occurrenceStatus",
  ]

  written_bad = 0
  with open(path, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=fields)
    w.writeheader()
    for i in range(n_rows):
      # Inject FK violations at roughly error_rate frequency
      inject = written_bad < n_bad and random.random() < error_rate
      if inject:
        written_bad += 1
        # Rotate which FK is broken so all three get coverage
        turn = written_bad % 3
        did = random.choice(bad_dataset) if turn == 0 else random.choice(dataset_ids)
        tid = random.choice(bad_taxon)   if turn == 1 else random.choice(taxon_ids)
        aid = random.choice(bad_agent)   if turn == 2 else random.choice(agent_ids)
      else:
        did = random.choice(dataset_ids)
        tid = random.choice(taxon_ids)
        aid = random.choice(agent_ids)

      w.writerow({
        "occurrenceID":   str(uuid.uuid4()),
        "datasetID":      did,
        "taxonID":        tid,
        "agentID":        aid,
        "decimalLatitude":  round(random.uniform(-90,  90),  6),
        "decimalLongitude": round(random.uniform(-180, 180), 6),
        "eventDate":        f"20{random.randint(0,24):02d}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
        "basisOfRecord":    random.choice(BASIS),
        "countryCode":      random.choice(COUNTRIES),
        "individualCount":  random.randint(1, 50),
        "coordinateUncertaintyInMeters": random.choice([None, 10, 100, 1000]),
        "occurrenceStatus": random.choice(["PRESENT","ABSENT"]),
      })

  print(f"  ✓ occurrence chunk {chunk_id} ({n_rows:,} rows, ~{written_bad} FK violations) → {path}")
  return path

# ---------------------------------------------------------------------------
# datapackage.json writer
# ---------------------------------------------------------------------------

def write_datapackage(out_dir, occ_paths):
  chunk_files = [os.path.basename(p) for p in sorted(occ_paths)]
  dp = {
    "name": "dwc-star-test",
    "profile": "tabular-data-package",
    "resources": [
      {
        "name": "dataset",
        "path": "dataset.csv",
        "profile": "tabular-data-resource",
        "schema": {
          "fields": [
            {"name": "datasetID",   "type": "string", "constraints": {"required": True}},
            {"name": "datasetName", "type": "string"},
            {"name": "ownerOrg",    "type": "string"},
            {"name": "license",     "type": "string"},
            {"name": "pubDate",     "type": "date"},
          ],
          "primaryKey": "datasetID"
        }
      },
      {
        "name": "taxon",
        "path": "taxon.csv",
        "profile": "tabular-data-resource",
        "schema": {
          "fields": [
            {"name": "taxonID",        "type": "string", "constraints": {"required": True}},
            {"name": "scientificName",  "type": "string"},
            {"name": "taxonRank",       "type": "string"},
            {"name": "kingdom",         "type": "string"},
            {"name": "class",           "type": "string"},
            {"name": "order",           "type": "string"},
            {"name": "family",          "type": "string"},
          ],
          "primaryKey": "taxonID"
        }
      },
      {
        "name": "agent",
        "path": "agent.csv",
        "profile": "tabular-data-resource",
        "schema": {
          "fields": [
            {"name": "agentID",  "type": "string", "constraints": {"required": True}},
            {"name": "fullName", "type": "string"},
            {"name": "role",     "type": "string"},
            {"name": "email",    "type": "string", "format": "email"},
            {"name": "orcid",    "type": "string"},
          ],
          "primaryKey": "agentID"
        }
      },
      {
        "name": "occurrence",
        "path": chunk_files,
        "profile": "tabular-data-resource",
        "schema": {
          "fields": [
            {"name": "occurrenceID",   "type": "string", "constraints": {"required": True}},
            {"name": "datasetID",      "type": "string", "constraints": {"required": True}},
            {"name": "taxonID",        "type": "string", "constraints": {"required": True}},
            {"name": "agentID",        "type": "string", "constraints": {"required": True}},
            {"name": "decimalLatitude",  "type": "number", "constraints": {"minimum": -90,  "maximum": 90}},
            {"name": "decimalLongitude", "type": "number", "constraints": {"minimum": -180, "maximum": 180}},
            {"name": "eventDate",        "type": "date"},
            {"name": "basisOfRecord",    "type": "string"},
            {"name": "countryCode",      "type": "string"},
            {"name": "individualCount",  "type": "integer"},
            {"name": "coordinateUncertaintyInMeters", "type": "integer"},
            {"name": "occurrenceStatus", "type": "string"},
          ],
          "primaryKey": "occurrenceID",
          "foreignKeys": [
            {
              "fields": ["datasetID"],
              "reference": {"resource": "dataset", "fields": ["datasetID"]}
            },
            {
              "fields": ["taxonID"],
              "reference": {"resource": "taxon",   "fields": ["taxonID"]}
            },
            {
              "fields": ["agentID"],
              "reference": {"resource": "agent",   "fields": ["agentID"]}
            }
          ]
        }
      }
    ]
  }
  out = os.path.join(out_dir, "datapackage.json")
  with open(out, "w") as f:
    json.dump(dp, f, indent=2, default=str)
  print(f"  ✓ datapackage.json → {out}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def write_csv(path, rows):
  if not rows: return
  with open(path, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=rows[0].keys())
    w.writeheader()
    w.writerows(rows)

if __name__ == "__main__":
  p = argparse.ArgumentParser()
  p.add_argument("--occurrences", type=int, default=10_000_000)
  p.add_argument("--datasets",    type=int, default=10_000)
  p.add_argument("--taxa",        type=int, default=50_000)
  p.add_argument("--agents",      type=int, default=5_000)
  p.add_argument("--error-rate",  type=float, default=0.01,
                 help="Fraction of occurrence rows with a dangling FK (e.g. 0.01 = 1%%)")
  p.add_argument("--out",         default="./dwc_star")
  p.add_argument("--workers",     type=int, default=os.cpu_count())
  p.add_argument("--chunk",       type=int, default=1_000_000)
  args = p.parse_args()

  os.makedirs(args.out, exist_ok=True)

  # 1. Generate dimension tables (single-threaded, small)
  print(f"Generating {args.datasets:,} datasets...")
  datasets = gen_datasets(args.datasets)
  write_csv(os.path.join(args.out, "dataset.csv"), datasets)

  print(f"Generating {args.taxa:,} taxa...")
  taxa = gen_taxa(args.taxa)
  write_csv(os.path.join(args.out, "taxon.csv"), taxa)

  print(f"Generating {args.agents:,} agents...")
  agents = gen_agents(args.agents)
  write_csv(os.path.join(args.out, "agent.csv"), agents)

  # 2. Extract ID pools to pass to workers
  dataset_ids = [r["datasetID"] for r in datasets]
  taxon_ids   = [r["taxonID"]   for r in taxa]
  agent_ids   = [r["agentID"]   for r in agents]

  # 3. Generate occurrence chunks in parallel
  n = args.occurrences
  chunk_args = [
    (i, min(args.chunk, n - i * args.chunk), args.out,
     dataset_ids, taxon_ids, agent_ids, args.error_rate)
    for i in range(-(-n // args.chunk))
  ]
  print(f"\nGenerating {n:,} occurrences across {len(chunk_args)} chunks ({args.workers} workers)...")
  with Pool(args.workers) as pool:
    occ_paths = pool.map(write_occurrence_chunk, chunk_args)

  # 4. Write datapackage.json
  write_datapackage(args.out, occ_paths)
  print(f"\n✅ Done. Output in {args.out}/")
