from sklearn.decomposition import PCA
from mpl_toolkits import mplot3d
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import pandas as pd

# Carico il file csv
root = "C:\\Users\\Gianluca\\Desktop\\result"
data = pd.read_csv(root+"\\cluster.csv\\cluster.csv")

#data.head()
#print(data)

pca = PCA(n_components=3)

features = [
      "Pace",
      "Acceleration",
      "Sprint Speed",
      "Dribbling",
      "Agility",
      "Balance",
      "Reactions",
      "Ball Control",
      "Composure",
      "Shooting",
      "Positioning",
      "Finishing",
      "Shot Power",
      "Long Shots",
      "Volleys",
      "Penalties",
      "Passing",
      "Vision",
      "Crossing",
      "Free Kick",
      "Short Pass",
      "Long Pass",
      "Pass Curve",
      "Defending",
      "Interceptions",
      "Heading",
      "Marking",
      "Standing Tackle",
      "Sliding Tackle",
      "Physicality",
      "Jumping",
      "Stamina",
      "Strength",
      "Aggression",
      "Diving",
      "Reflexes",
      "Handling",
      "Speed",
      "Kicking",
      "Positoning"]

x = data.loc[:,features].values
y = data.loc[:,['prediction']].values
name = data.loc[:,['Player Name']].values

x = StandardScaler().fit_transform(x)    

principalComponents = pca.fit_transform(x)

principalDf = pd.DataFrame(data = principalComponents
             , columns = ['principal component 1', 'principal component 2', 'principal component 3'])

finalDf = pd.concat([principalDf, data[['prediction']]], axis = 1)

fig = plt.figure(figsize = (8,8))
#ax = fig.add_subplot(1,1,1) 
ax = plt.axes(projection='3d')
ax.set_xlabel('Principal Component 1', fontsize = 15)
ax.set_ylabel('Principal Component 2', fontsize = 15)
#ax.set_zlabel('Principal Component 3', fontsize = 15)
ax.set_title('2 component PCA', fontsize = 20)

#targets = ["GK","LB","CB","RB","CDM","CM","CAM","LM","LW","LF","RM","RW","RF","CF","ST"]
targets = range(15)
colors = ['r', 'g', 'b','y','m','g','c','k','steelblue','lavender','darkblue','goldenrod','linen','hotpink','orchid']
for target, color in zip(targets,colors):
    indicesToKeep = finalDf['prediction'] == target
    ax.scatter3D(finalDf.loc[indicesToKeep, 'principal component 1']
               , finalDf.loc[indicesToKeep, 'principal component 2']
               ,finalDf.loc[indicesToKeep, 'principal component 3']
               , c = color
               , s = 50)
ax.legend(targets)
ax.grid()

plt.show()

