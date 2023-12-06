import pandas as pd
import matplotlib.pyplot as plt
import json

series = {}
months = ["January", "February", "March"]

for partition in range(4):
    filename = f'/files/partition-{partition}.json'
    try:
        with open(filename, 'r') as f:
            partition_data = json.load(f)
            for key in partition_data.keys():
                if key in months:
                    partion_month = key
                    partion_year = list(partition_data[partion_month].keys())[0]
                    if len(list(partition_data[partion_month].keys())) > 1:
                        partion_year = partition_data[partion_month][list(partition_data[partion_month].keys())[-1]]["end"][:4]
                    partition_avg = partition_data[partion_month][partion_year]["avg"]
                    series_key = f'{partion_month}-{partion_year}'
                    series.update({series_key: partition_avg})
    except FileNotFoundError:
        pass
print(series)
month_series = pd.Series(series)

fig, ax = plt.subplots()
month_series.plot.bar(ax=ax)
ax.set_ylabel('Avg. Max Temperature')
plt.tight_layout()
plt.savefig("/files/month.svg")