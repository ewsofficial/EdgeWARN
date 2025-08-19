import xarray as xr
from util import file as fs
from . import detect
import numpy

nldn_file = fs.latest_nldn(1)[0]
echotop_file = fs.latest_echotop18(1)[0]
glm_file = fs.latest_glm(1)[0]
preciprate_file = fs.latest_preciprate(1)[0]
qpe_file = fs.latest_qpe15(1)[0]
probsevere_file = fs.latest_probsevere(1)[0]
rtma_file = fs.latest_rtma(1)[0]

