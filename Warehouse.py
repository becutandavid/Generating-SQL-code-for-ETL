import numpy as np
import pandas as pd
from Attribute import Attribute
from Dimension import Dimension


if __name__=="__main__":
    
    dim = Dimension("dim_admins")
    print(dim.ddl())
