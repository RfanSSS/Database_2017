import sys
import os
#from Database                import Database
from Utils.WorkloadGenerator import WorkloadGenerator
from Storage.File            import StorageFile
from Storage.Page            import Page
from Storage.SlottedPage     import SlottedPage
import matplotlib
import matplotlib.pyplot as plt

# Path to the folder containing csvs (on ugrad cluster)
dataDir = './tpch-sf0.01/'

# Pick a page class, page size, scale factor, and workload mode:
#StorageFile.defaultPageClass = Page   # Contiguous Page
#pageSize = 32768                       # 4Kb
#scaleFactor = 0.2                     # Half of the data
#workloadMode = 4                      # Sequential Reads

# Run! Throughput will be printed afterwards.
# Note that the reported throughput ignores the time
# spent loading the dataset.
def diskUsage():
    diskUsage = 0
    for file in os.listdir('./data/'):
        if file.endswith('.rel'):
            diskUsage += os.path.getsize(os.path.join('./data/', file))
        return diskUsage / 1024

def runWorkload():
    
    scale = [0.2, 0.4, 0.6, 0.8, 1.0]
    throughput_list = []
    # Initialize the plot
    plt.figure(figsize = (16,10))
    plt.xlabel("Scale Factor")
    plt.ylabel("Throughput")
    plt.title("ThroughPut Comparison")
    for workloadMode in [1,2,3,4]:
        for pageSize in [4096, 32768]:
            for PageClass in [Page, SlottedPage]:
                StorageFile.defaultPageClass = PageClass
                throughput_list = []
                for scaleFactor in [0.2,0.4,0.6,0.8,1.0]:
                    #print(StorageFile.defaultPageClass, scaleFactor, pageSize, workloadMode)
                    wg = WorkloadGenerator()
                    (Tuples, Throughput, time) = wg.runWorkload(dataDir, scaleFactor, pageSize, workloadMode)
                    throughput_list.append(Throughput)
                if StorageFile.defaultPageClass == Page:
                    plt.plot(scale, throughput_list, label = "WorkMode: "+str(workloadMode)+", "+ "pageSize: " + str(pageSize/1024) + "Kb" + ", "+"Page", linewidth = 2)
                elif StorageFile.defaultPageClass == SlottedPage:
                    plt.plot(scale, throughput_list, label = "WorkMode: "+str(workloadMode)+", "+ "pageSize: " + str(pageSize/1024) + "Kb" + ", "+"SlottedPage", linewidth = 2)                    
    plt.legend(loc = 'upper right', fancybox=True, framealpha=0.2)
    plt.savefig("myfig")

runWorkload()

#StorageFile.defaultPageClass = SlottedPage
#runWorkload(Pageclass = "SlottedPage")
