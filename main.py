"""
@Author: Göktuğ Öcal
@Date: 16.11.2022
@Credit: Crossing Paths
"""

#-------- MAPREDUCE SCHEMA --------
#
#        ┌───────────────┐
#        │   LOAD DATA   │
#        └───────┬───────┘
#        ┌───────┴───────┐
# ┌──────▼─────┐  ┌──────▼─────┐
# │   MAPPER   │  │   MAPPER   │
# └──────┬─────┘  └──────┬─────┘
# ┌──────▼─────┐  ┌──────▼─────┐
# │  COMBINER  │  │  COMBINER  │
# └──────┬─────┘  └──────┬─────┘
#        └───────┬───────┘
#        ┌───────▼───────┐
#        │   LOAD DATA   |
#        └───────────────┘
#----------------------------------

from mapreduce import *

data = loadData()

mr = MapReduce()
mr.set_data(data)
res = mr.run()

print(res)