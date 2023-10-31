import os
from cdse import CDSE
import sys

start_date = "2020-10-01"
end_date = "2020-11-05"

footprint = 'jylland_skybrud/POLYGON.shp'   #dummy shape
footprint = ['32TPT', '31RFL']  #dummy tiles

output_dir = 'output/'
# output_dir = "D:/s2_jylland/"
cloudcover = 50                   #max percent allowed cloud cover on images
max_records = None                #max records retireved in query. None is equal to infinity

# CHANGE SENTINEL TYPE IN ASSOCIATED CLASS FILE FOR NOW

os.makedirs(output_dir, exist_ok = True)

cdse = CDSE()
cdse.set_collection("Sentinel2")
cdse.set_processing_level("S2MSI2A")
footprint = CDSE.process_footprint(footprint)
features = cdse.query(start_date = start_date, end_date = end_date, footprint = footprint, cloudcover = cloudcover, max_records = max_records)

cdse.download_features(features[:5], output_dir)    

sys.exit()

# # # handy debugging stuff
# # from pprint import pprint
# print(len(features))
# # # for x in features:
# # #       print(x['properties']['productType'])
# # # pprint(features[0]['properties'])
# # sys.exit()

# for feature in features:
#     cdse.stream_to_dir(feature, output_dir)