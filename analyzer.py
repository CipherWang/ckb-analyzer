#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from worker.data_walker import BlockHeaderParser, BlockWalker
from worker.data_parsers import ParserBlockTime
import numpy as np
from matplotlib import pyplot as plt

bw = BlockWalker("sample.sqlite3")
pb = ParserBlockTime()
bw.register_parser(pb)
bw.walk_through()

pb.show()