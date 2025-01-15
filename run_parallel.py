import dask.delayed
from dask import compute
from dask.distributed import Client


# Initialize the Dask client to enable parallel execution
client = Client()


@dask.delayed
def run_analysis_parallel(hdc):
  run_analysis(hdc)

from multiprocessing.pool import ThreadPool
dask.config.set(pool=ThreadPool(5))


start = timeit.default_timer()
# Create a list of delayed tasks
results = [run_analysis_parallel(hdc) for hdc in hdcs_all[1:20]]

# Compute the results (this will trigger the parallel execution)
compute(*results)
end = timeit.default_timer()


# Close the client after computation is done
client.close()


hdcs = pd.read_csv('input_data/hdc_to_run.csv', dtype={'hdc_new' : str})


hdcs_all = hdcs["hdc_new"].tolist()


def run_analysis(hdc):
  return regional_analysis(
    hdc,
    to_test=['pnrt'],
    analyse=True,
    summarize=False,
    jurisdictions=False,
    current_year=2024,
    prep=False,
  )




start = timeit.default_timer()
run_analysis("00178")
end = timeit.default_timer()
