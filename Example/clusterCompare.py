import pandas

cluster = pandas.read_csv("cluster.csv")

dif_c = pandas.read_csv("difensori_c.csv")
#por_c = pandas.read_csv("portieri.csv")
#att_c = pandas.read_csv("attaccanti.csv")
#cen_c = pandas.read_csv("centrocampisti.csv")


dif_k = cluster[cluster.prediction == 3]

for x in range(0,4):
	print(cluster[cluster.prediction == x])
	

#print(dif_c)

for x in dif_c.itertuples():
	tmp = x.Position
	tmp_dif = dif_k.loc[dif_k['Position'] == tmp]
	print(tmp_dif)
	tmp_count = 1
	print(tmp_count)
	tmp2 = x.count
	print(tmp)
	print(tmp2)
	#print(x)
	