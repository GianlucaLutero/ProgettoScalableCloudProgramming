import pandas

cluster = pandas.read_csv("cluster.csv")

dif_c = pandas.read_csv("difensori_c.csv")
#por_c = pandas.read_csv("portieri.csv")
#att_c = pandas.read_csv("attaccanti.csv")
#cen_c = pandas.read_csv("centrocampisti.csv")


dif_k = cluster[cluster.prediction == 0]

for x in range(0,4):
	print(cluster[cluster.prediction == x])
	

print(dif_c)

control_total = 0
analysis_total = 0

for x in dif_c.itertuples():
	tmp = x.Position
	tmp_dif = dif_k.loc[dif_k['Position'] == tmp,'count'].values
	print(tmp_dif[0])
	analysis_total += tmp_dif
	control_total += x.count
	#print(x)
	

print("Success ratio")
print(analysis_total/control_total)	