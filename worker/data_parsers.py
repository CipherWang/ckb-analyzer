from worker.data_walker import BlockHeaderParser
import numpy as np
from matplotlib import pyplot as plt

def smooth_window(data_set, window_len):
    sindex = 0
    sarray = []
    smooth_data = []
    for tt in data_set:
        if len(sarray) < window_len:
            sarray.append(tt)
        else:
            sarray[sindex] = tt
            sindex = (sindex+1)%window_len
        smooth_data.append(np.sum(sarray) / len(sarray))
    return smooth_data

class ParserBlockTime(BlockHeaderParser):
    def __init__(self):
        super().__init__()
        self.last_timestamp = 0
        self.block_intervals = []

    def work(self, record):
        (blknum, timestamp) = (record['number'], record['timestamp'])
        timestamp = timestamp / 1000.0
        if self.last_timestamp != 0:
            self.block_intervals.append((blknum, timestamp - self.last_timestamp))
        self.last_timestamp = timestamp

    def analyze(self):
        data = np.array(self.block_intervals)[:,1]
        blkcount = 0
        taccum = 0.0
        hourly_average = []
        for ti in data:
            blkcount += 1
            taccum += ti
            if taccum >= 3600:
                average = taccum / blkcount
                hourly_average.append(average)
                blkcount = 0
                taccum = 0

        self.hourly_average = hourly_average
        self.smooth_data_daily = smooth_window(hourly_average, 24)
        self.smooth_data_weekly = smooth_window(hourly_average, 24*7)

    def export(self):
        return {
            'block_intervals': self.block_intervals,
            'hourly': self.hourly_average,
            'daily': self.smooth_data_daily,
            'weekly': self.smooth_data_weekly
        }

    def show(self):
        data = self.export()
        plt.plot(np.array(data['block_intervals'])[:,1], '.', ms=0.5)
        plt.show()
        plt.plot(data['daily'], 'b', lw=1)
        plt.plot(data['weekly'], 'r', lw=1.5)
        plt.xlim(0, len(data['hourly']))
        plt.ylim(0)
        plt.show()
