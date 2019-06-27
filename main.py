import requests
import json
from string import Template
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-q", "--query", type=str, help="query string")
args = parser.parse_args()

url = "http://reach-api.nrnb-docker.ucsd.edu/"

query_str = """
{
  allDocuments(entity_text: "$entity_text") {
    pmc_id
    evidence
    extracted_information {
      participant_b {
        identifier
        entity_type
        entity_text
      }
      participant_a {
        identifier
        entity_type
        entity_text
      }
    }
  }
}
"""

query = Template(query_str)

if args.query is not None:
    entity_text = args.query
else:
    entity_text = 'celsr2'

query = query.substitute(entity_text=entity_text)
#print(query)

r = requests.post(url, params={'query': query})
# print(r.text)

parsed_json = json.loads(r.text)
print(json.dumps(parsed_json, indent=2))
