import pandas as pd
import numpy as np
df = pd.DataFrame(np.random.rand(1000, 10), columns=[f"col_{i}" for i in range(10)])
df.to_parquet('/mnt/scratch/yuzhuyu/parquet/criteo_1TB/test.parquet')