
import pandas as pd

chunker = pd.read_json("/media/andy/Data/python/db/beijing.geojson", chunksize=1000000, lines=True)
for piece in chunker:
    print(type(piece))
    # <class 'pandas.core.frame.DataFrame'>
    print(len(piece))
    # 5