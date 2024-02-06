import matplotlib.pyplot as plt
import numpy as np

# Data
nbMachine = [2, 3, 5]
times = [15.760, 10.277, 5.99]

times2 = [5.537, 9.18, 13.270, 17.118]
fileSize = [2.7, 5.5, 8, 11]

times3 = [7.412, 7.489, 7.565, 8.023]
fragmentSize = [100000000, 50000000, 25000000, 10000000]

# Graph 1
fig, ax = plt.subplots()
ax.set_xlabel("Number of machines")
ax.set_ylabel("Execution time")
ax.set_title("Execution time for a 10GB file")
ax.plot(nbMachine, times, label="Execution time")
ax.legend()
plt.savefig("images/simultaneous10gb.png")
plt.show()
plt.close()

# Graph 2
fig, ax = plt.subplots()
ax.set_xlabel("File size")
ax.set_ylabel("Execution time")
ax.set_title("Execution time according to file size \n with 2 machines and 100,000,000 character fragments")
ax.plot(fileSize, times2, label="Execution time")
ax.legend()
plt.savefig("images/executionTimeVsFileSize.png")
plt.show()
plt.close()

# Graph 3
fig, ax = plt.subplots()
ax.set_xlabel("Fragment size")
ax.set_ylabel("Execution time")
ax.set_title("Execution time according to fragment size \n with 2 machines and a 2.5GB file")
ax.plot(fragmentSize, times3, label="Execution time")
ax.legend()
plt.savefig("images/executionTimeVsFragmentSize.png")
plt.show()
plt.close()
