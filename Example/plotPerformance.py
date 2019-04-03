import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


data = pd.read_csv("performance.csv")

x = data.loc[:,['K']].values
y = data.loc[:,['Score']].values
y2 = data.loc[:,['WSS']].values


plt.subplot(2,1,1)
plt.plot(x, y, 'o-')
plt.title('Cluster analysis')
plt.ylabel('Average error')

plt.subplot(2,1,2)
plt.plot(x,y2,'o-')
plt.ylabel('sum of squared error')

plt.show()